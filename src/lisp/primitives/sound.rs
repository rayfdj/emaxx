//! sound.c's `play-sound-internal' for GNU/Linux: RIFF-WAVE and Sun audio
//! files played on an ALSA device when one opens (libasound, loaded on
//! first use as GNU's build links it), else on the Voxware device.

use super::*;
use crate::lisp::types::Kind;
use std::ffi::{CStr, CString, c_char, c_int, c_long, c_uint, c_ulong, c_void};
use std::io::{Read, Seek, SeekFrom};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// sizeof (struct wav_header) and sizeof (struct au_header).
const WAV_HEADER_BYTES: usize = 44;
const AU_HEADER_BYTES: usize = 24;
/// MAX_SOUND_HEADER_BYTES.
const MAX_SOUND_HEADER_BYTES: usize = WAV_HEADER_BYTES;
/// config.h's DEFAULT_SOUND_DEVICE and sound.c's DEFAULT_ALSA_SOUND_DEVICE.
const DEFAULT_SOUND_DEVICE: &str = "/dev/dsp";
const DEFAULT_ALSA_SOUND_DEVICE: &str = "default";

/// struct wav_header's fields, little-endian in the file.
struct WavHeader {
    sample_rate: u32,
    bytes_per_second: u32,
    channels: u16,
    sample_size: u16,
    precision: u16,
    data_length: u32,
}

/// struct au_header's fields, big-endian in the file.
struct AuHeader {
    data_offset: u32,
    encoding: u32,
    sample_rate: u32,
    channels: u32,
}

/// enum au_encoding.
const AU_ENCODING_ULAW_8: u32 = 1;
const AU_ENCODING_8: u32 = 2;
const AU_ENCODING_16: u32 = 3;
const AU_ENCODING_24: u32 = 4;
const AU_ENCODING_32: u32 = 5;
const AU_ENCODING_IEEE32: u32 = 6;
const AU_ENCODING_IEEE64: u32 = 7;
const AU_ENCODING_ALAW_8: u32 = 27;

enum SoundType {
    Riff(WavHeader),
    SunAudio(AuHeader),
}

/// struct sound: the file read from, or the data string's bytes.
enum SoundSource {
    File(crate::file_system::File),
    Data(Vec<u8>),
}

fn sound_error(message: impl Into<String>) -> LispError {
    LispError::Signal(message.into())
}

/// sound_perror: MSG with errno's text when there is one.
fn sound_perror(msg: &str, errno: i32) -> LispError {
    if errno != 0 {
        sound_error(format!("{msg}: {}", strerror(errno)))
    } else {
        sound_error(msg)
    }
}

fn strerror(errno: i32) -> String {
    // SAFETY: strerror returns a NUL-terminated message for any errno.
    unsafe { CStr::from_ptr(libc::strerror(errno)) }
        .to_string_lossy()
        .into_owned()
}

fn last_errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

/// The attributes parse_sound accepts: SOUND is (sound . PLIST) with a
/// string :file or :data, a :volume in [0, 100] or [0.0, 1.0] and a
/// string :device, each optional but one of the first two.
struct SoundAttributes {
    file: Value,
    data: Value,
    device: Option<String>,
    volume: Value,
}

fn parse_sound(sound: Value) -> Result<Option<SoundAttributes>, LispError> {
    let Some((head, plist)) = sound.cons_values() else {
        return Ok(None);
    };
    if head != Value::symbol("sound") {
        return Ok(None);
    }
    let get = |key: &str| super::strings::lisp_plist_get(plist, key);
    let (file, data, device, volume) = (get(":file"), get(":data"), get(":device"), get(":volume"));
    if !file.is_string() && !data.is_string() {
        return Ok(None);
    }
    let volume_ok = match volume.kind() {
        Kind::Nil => true,
        Kind::Integer(volume) => (0..=100).contains(&volume),
        Kind::Float(volume) => (0.0..=1.0).contains(&volume.get()),
        _ => false,
    };
    if !volume_ok || !(device.is_nil() || device.is_string()) {
        return Ok(None);
    }
    let device = if device.is_string() {
        Some(string_text(&device)?)
    } else {
        None
    };
    Ok(Some(SoundAttributes {
        file,
        data,
        device,
        volume,
    }))
}

/// find_sound_type: wav_init, then au_init, over the header bytes.
fn find_sound_type(header: &[u8]) -> Result<SoundType, LispError> {
    let le32 = |at: usize| u32::from_le_bytes(header[at..at + 4].try_into().expect("4 bytes"));
    let le16 = |at: usize| u16::from_le_bytes(header[at..at + 2].try_into().expect("2 bytes"));
    let be32 = |at: usize| u32::from_be_bytes(header[at..at + 4].try_into().expect("4 bytes"));
    if header.len() >= WAV_HEADER_BYTES && header.starts_with(b"RIFF") {
        return Ok(SoundType::Riff(WavHeader {
            channels: le16(22),
            sample_rate: le32(24),
            bytes_per_second: le32(28),
            sample_size: le16(32),
            precision: le16(34),
            data_length: le32(40),
        }));
    }
    if header.len() >= AU_HEADER_BYTES && header.starts_with(b".snd") {
        return Ok(SoundType::SunAudio(AuHeader {
            data_offset: be32(4),
            encoding: be32(12),
            sample_rate: be32(16),
            channels: be32(20),
        }));
    }
    Err(sound_error("Unknown sound format"))
}

/// The parts of struct sound_device every driver reads.
struct DeviceParams {
    file: Option<String>,
    format: c_int,
    volume: c_int,
    sample_size: c_int,
    sample_rate: c_int,
    bps: c_int,
    channels: c_int,
}

/// The two drivers' struct sound_device state.
enum Driver {
    Alsa(Box<AlsaDevice>),
    Vox(Option<OwnedFd>),
}

struct SoundDevice {
    params: DeviceParams,
    driver: Driver,
}

impl Drop for SoundDevice {
    /// sound_cleanup's close: drain or sync the device, then close it.
    fn drop(&mut self) {
        match &mut self.driver {
            Driver::Alsa(device) => device.close(),
            Driver::Vox(fd) => {
                if let Some(fd) = fd.take() {
                    // SAFETY: an open descriptor; SNDCTL_DSP_SYNC takes no
                    // argument.
                    unsafe { libc::ioctl(fd.as_raw_fd(), SNDCTL_DSP_SYNC, 0) };
                }
            }
        }
    }
}

impl SoundDevice {
    fn device_name(&self, default: &str) -> String {
        self.params
            .file
            .clone()
            .unwrap_or_else(|| default.to_string())
    }

    fn open(&mut self) -> Result<(), LispError> {
        let file = match &self.driver {
            Driver::Alsa(_) => self.device_name(DEFAULT_ALSA_SOUND_DEVICE),
            Driver::Vox(_) => self.device_name(DEFAULT_SOUND_DEVICE),
        };
        match &mut self.driver {
            Driver::Alsa(device) => device.open(&file),
            Driver::Vox(fd) => {
                *fd = Some(vox_open(&file)?);
                Ok(())
            }
        }
    }

    fn choose_format(&mut self, sound: &SoundType) -> Result<(), LispError> {
        let alsa = matches!(self.driver, Driver::Alsa(_));
        self.params.format = match sound {
            SoundType::Riff(header) => match (header.precision, alsa) {
                (8, true) => SND_PCM_FORMAT_U8,
                (16, true) => SND_PCM_FORMAT_S16_LE,
                (8, false) => AFMT_U8,
                (16, false) => AFMT_S16_LE,
                _ => return Err(sound_error("Unsupported WAV file format")),
            },
            SoundType::SunAudio(header) if alsa => match header.encoding {
                AU_ENCODING_ULAW_8 => SND_PCM_FORMAT_MU_LAW,
                AU_ENCODING_ALAW_8 => SND_PCM_FORMAT_A_LAW,
                AU_ENCODING_IEEE32 => SND_PCM_FORMAT_FLOAT_BE,
                AU_ENCODING_IEEE64 => SND_PCM_FORMAT_FLOAT64_BE,
                AU_ENCODING_8 => SND_PCM_FORMAT_S8,
                AU_ENCODING_16 => SND_PCM_FORMAT_S16_BE,
                AU_ENCODING_24 => SND_PCM_FORMAT_S24_BE,
                AU_ENCODING_32 => SND_PCM_FORMAT_S32_BE,
                _ => return Err(sound_error("Unsupported AU file format")),
            },
            SoundType::SunAudio(header) => match header.encoding {
                AU_ENCODING_ULAW_8 | AU_ENCODING_IEEE32 | AU_ENCODING_IEEE64 => AFMT_MU_LAW,
                AU_ENCODING_8 | AU_ENCODING_16 | AU_ENCODING_24 | AU_ENCODING_32 => AFMT_S16_LE,
                _ => return Err(sound_error("Unsupported AU file format")),
            },
        };
        Ok(())
    }

    fn configure(&mut self, interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
        let file = self.device_name(DEFAULT_ALSA_SOUND_DEVICE);
        match &mut self.driver {
            Driver::Alsa(device) => device.configure(&self.params, &file),
            Driver::Vox(fd) => {
                let fd = fd.as_ref().expect("vox device opened").as_raw_fd();
                vox_configure(fd, &mut self.params, interp, env)
            }
        }
    }

    fn period_size(&self) -> usize {
        match &self.driver {
            Driver::Alsa(device) => device.period_size(&self.params),
            Driver::Vox(_) => 2048,
        }
    }

    fn write(&mut self, buffer: &[u8]) -> Result<(), LispError> {
        match &mut self.driver {
            Driver::Alsa(device) => device.write(&self.params, buffer),
            Driver::Vox(fd) => {
                let fd = fd.as_ref().expect("vox device opened").as_raw_fd();
                vox_write(fd, buffer)
            }
        }
    }
}

/// soundcard.h's ioctls and formats.
const SNDCTL_DSP_SPEED: libc::c_ulong = 0xc004_5002;
const SNDCTL_DSP_STEREO: libc::c_ulong = 0xc004_5003;
const SNDCTL_DSP_SETFMT: libc::c_ulong = 0xc004_5005;
const SNDCTL_DSP_SYNC: libc::c_ulong = 0x5001;
const SOUND_MIXER_WRITE_PCM: libc::c_ulong = 0xc004_4d04;
const AFMT_MU_LAW: c_int = 0x01;
const AFMT_U8: c_int = 0x08;
const AFMT_S16_LE: c_int = 0x10;

fn open_write_only(file: &str) -> Result<OwnedFd, i32> {
    let path = CString::new(file).map_err(|_| libc::ENOENT)?;
    // SAFETY: a NUL-terminated path; the descriptor is owned from here.
    let fd = unsafe { libc::open(path.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC) };
    if fd < 0 {
        Err(last_errno())
    } else {
        // SAFETY: open returned a fresh descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

/// vox_init: the device must open for writing.
fn vox_init(params: &DeviceParams) -> bool {
    let file = params.file.as_deref().unwrap_or(DEFAULT_SOUND_DEVICE);
    open_write_only(file).is_ok()
}

fn vox_open(file: &str) -> Result<OwnedFd, LispError> {
    open_write_only(file).map_err(|errno| sound_perror(file, errno))
}

fn vox_configure(
    fd: c_int,
    params: &mut DeviceParams,
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    let ioctl = |request: libc::c_ulong, value: &mut c_int| {
        // SAFETY: each request reads and writes one int.
        unsafe { libc::ioctl(fd, request, value as *mut c_int) }
    };
    let wanted = params.format;
    if ioctl(SNDCTL_DSP_SETFMT, &mut params.format) < 0 || wanted != params.format {
        return Err(sound_perror("Could not set sound format", last_errno()));
    }
    let stereo = c_int::from(params.channels != 1);
    let mut value = stereo;
    if ioctl(SNDCTL_DSP_STEREO, &mut value) < 0 || value != stereo {
        return Err(sound_perror("Could not set stereo/mono", last_errno()));
    }
    if params.sample_rate > 0 {
        let wanted = params.sample_rate;
        if ioctl(SNDCTL_DSP_SPEED, &mut params.sample_rate) < 0 {
            return Err(sound_perror("Could not set sound speed", last_errno()));
        } else if wanted != params.sample_rate {
            // sound_warning's message1.
            super::call(
                interp,
                "message",
                &[
                    Value::string("%s"),
                    Value::string("Could not set sample rate"),
                ],
                env,
            )?;
        }
    }
    if params.volume > 0 {
        let volume = params.volume & 0xff;
        let mut volume = volume | (volume << 8);
        // This may fail if there is no mixer.
        ioctl(SOUND_MIXER_WRITE_PCM, &mut volume);
    }
    Ok(())
}

fn vox_write(fd: c_int, mut buffer: &[u8]) -> Result<(), LispError> {
    // emacs_write_sig: the whole buffer, retrying an interrupted write.
    while !buffer.is_empty() {
        // SAFETY: the pointer and length name the live slice.
        let written = unsafe { libc::write(fd, buffer.as_ptr().cast(), buffer.len()) };
        if written < 0 {
            let errno = last_errno();
            if errno == libc::EINTR {
                continue;
            }
            return Err(sound_perror("Error writing to sound device", errno));
        }
        buffer = &buffer[written as usize..];
    }
    Ok(())
}

/// alsa/pcm.h's values the driver uses.
const SND_PCM_STREAM_PLAYBACK: c_int = 0;
const SND_PCM_ACCESS_RW_INTERLEAVED: c_int = 3;
const SND_PCM_FORMAT_S8: c_int = 0;
const SND_PCM_FORMAT_U8: c_int = 1;
const SND_PCM_FORMAT_S16_LE: c_int = 2;
const SND_PCM_FORMAT_S16_BE: c_int = 3;
const SND_PCM_FORMAT_S24_BE: c_int = 7;
const SND_PCM_FORMAT_S32_BE: c_int = 11;
const SND_PCM_FORMAT_FLOAT_BE: c_int = 15;
const SND_PCM_FORMAT_FLOAT64_BE: c_int = 17;
const SND_PCM_FORMAT_MU_LAW: c_int = 20;
const SND_PCM_FORMAT_A_LAW: c_int = 21;
const SND_MIXER_SCHN_LAST: c_int = 31;

type Pcm = *mut c_void;
type ErrorHandler = Option<unsafe extern "C" fn()>;

/// The libasound entry points sound.c calls.
struct Alsa {
    _library: libloading::Library,
    lib_error_set_handler: unsafe extern "C" fn(ErrorHandler) -> c_int,
    strerror: unsafe extern "C" fn(c_int) -> *const c_char,
    pcm_open: unsafe extern "C" fn(*mut Pcm, *const c_char, c_int, c_int) -> c_int,
    pcm_close: unsafe extern "C" fn(Pcm) -> c_int,
    pcm_drain: unsafe extern "C" fn(Pcm) -> c_int,
    pcm_prepare: unsafe extern "C" fn(Pcm) -> c_int,
    pcm_resume: unsafe extern "C" fn(Pcm) -> c_int,
    pcm_writei: unsafe extern "C" fn(Pcm, *const c_void, c_ulong) -> c_long,
    pcm_format_size: unsafe extern "C" fn(c_int, usize) -> isize,
    hw_params_malloc: unsafe extern "C" fn(*mut *mut c_void) -> c_int,
    hw_params_free: unsafe extern "C" fn(*mut c_void),
    hw_params_any: unsafe extern "C" fn(Pcm, *mut c_void) -> c_int,
    hw_params_set_access: unsafe extern "C" fn(Pcm, *mut c_void, c_int) -> c_int,
    hw_params_set_format: unsafe extern "C" fn(Pcm, *mut c_void, c_int) -> c_int,
    hw_params_set_rate_near:
        unsafe extern "C" fn(Pcm, *mut c_void, *mut c_uint, *mut c_int) -> c_int,
    hw_params_set_channels: unsafe extern "C" fn(Pcm, *mut c_void, c_uint) -> c_int,
    hw_params: unsafe extern "C" fn(Pcm, *mut c_void) -> c_int,
    hw_params_get_period_size:
        unsafe extern "C" fn(*const c_void, *mut c_ulong, *mut c_int) -> c_int,
    hw_params_get_buffer_size: unsafe extern "C" fn(*const c_void, *mut c_ulong) -> c_int,
    sw_params_malloc: unsafe extern "C" fn(*mut *mut c_void) -> c_int,
    sw_params_free: unsafe extern "C" fn(*mut c_void),
    sw_params_current: unsafe extern "C" fn(Pcm, *mut c_void) -> c_int,
    sw_params_set_start_threshold: unsafe extern "C" fn(Pcm, *mut c_void, c_ulong) -> c_int,
    sw_params_set_avail_min: unsafe extern "C" fn(Pcm, *mut c_void, c_ulong) -> c_int,
    sw_params: unsafe extern "C" fn(Pcm, *mut c_void) -> c_int,
    mixer_open: unsafe extern "C" fn(*mut *mut c_void, c_int) -> c_int,
    mixer_attach: unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int,
    mixer_load: unsafe extern "C" fn(*mut c_void) -> c_int,
    mixer_selem_register: unsafe extern "C" fn(*mut c_void, *mut c_void, *mut *mut c_void) -> c_int,
    mixer_first_elem: unsafe extern "C" fn(*mut c_void) -> *mut c_void,
    mixer_elem_next: unsafe extern "C" fn(*mut c_void) -> *mut c_void,
    mixer_selem_has_playback_volume: unsafe extern "C" fn(*mut c_void) -> c_int,
    mixer_selem_get_playback_volume_range:
        unsafe extern "C" fn(*mut c_void, *mut c_long, *mut c_long) -> c_int,
    mixer_selem_set_playback_volume: unsafe extern "C" fn(*mut c_void, c_int, c_long) -> c_int,
    mixer_close: unsafe extern "C" fn(*mut c_void) -> c_int,
}

impl Alsa {
    fn load() -> Option<Self> {
        // SAFETY: libasound's initializers have no preconditions, and each
        // symbol is given the type of its alsa-lib prototype.
        unsafe {
            let library = libloading::Library::new("libasound.so.2").ok()?;
            macro_rules! sym {
                ($name:literal) => {
                    *library.get(concat!($name, "\0").as_bytes()).ok()?
                };
            }
            Some(Self {
                lib_error_set_handler: sym!("snd_lib_error_set_handler"),
                strerror: sym!("snd_strerror"),
                pcm_open: sym!("snd_pcm_open"),
                pcm_close: sym!("snd_pcm_close"),
                pcm_drain: sym!("snd_pcm_drain"),
                pcm_prepare: sym!("snd_pcm_prepare"),
                pcm_resume: sym!("snd_pcm_resume"),
                pcm_writei: sym!("snd_pcm_writei"),
                pcm_format_size: sym!("snd_pcm_format_size"),
                hw_params_malloc: sym!("snd_pcm_hw_params_malloc"),
                hw_params_free: sym!("snd_pcm_hw_params_free"),
                hw_params_any: sym!("snd_pcm_hw_params_any"),
                hw_params_set_access: sym!("snd_pcm_hw_params_set_access"),
                hw_params_set_format: sym!("snd_pcm_hw_params_set_format"),
                hw_params_set_rate_near: sym!("snd_pcm_hw_params_set_rate_near"),
                hw_params_set_channels: sym!("snd_pcm_hw_params_set_channels"),
                hw_params: sym!("snd_pcm_hw_params"),
                hw_params_get_period_size: sym!("snd_pcm_hw_params_get_period_size"),
                hw_params_get_buffer_size: sym!("snd_pcm_hw_params_get_buffer_size"),
                sw_params_malloc: sym!("snd_pcm_sw_params_malloc"),
                sw_params_free: sym!("snd_pcm_sw_params_free"),
                sw_params_current: sym!("snd_pcm_sw_params_current"),
                sw_params_set_start_threshold: sym!("snd_pcm_sw_params_set_start_threshold"),
                sw_params_set_avail_min: sym!("snd_pcm_sw_params_set_avail_min"),
                sw_params: sym!("snd_pcm_sw_params"),
                mixer_open: sym!("snd_mixer_open"),
                mixer_attach: sym!("snd_mixer_attach"),
                mixer_load: sym!("snd_mixer_load"),
                mixer_selem_register: sym!("snd_mixer_selem_register"),
                mixer_first_elem: sym!("snd_mixer_first_elem"),
                mixer_elem_next: sym!("snd_mixer_elem_next"),
                mixer_selem_has_playback_volume: sym!("snd_mixer_selem_has_playback_volume"),
                mixer_selem_get_playback_volume_range: sym!(
                    "snd_mixer_selem_get_playback_volume_range"
                ),
                mixer_selem_set_playback_volume: sym!("snd_mixer_selem_set_playback_volume"),
                mixer_close: sym!("snd_mixer_close"),
                _library: library,
            })
        }
    }

    /// alsa_sound_perror.
    fn error(&self, msg: &str, err: c_int) -> LispError {
        // SAFETY: snd_strerror returns a static message for any code.
        let text = unsafe { CStr::from_ptr((self.strerror)(err)) };
        sound_error(format!("{msg}: {}", text.to_string_lossy()))
    }
}

/// snd_error_quiet: alsa_init's probe reports nothing.
unsafe extern "C" fn snd_error_quiet() {}

/// struct alsa_params.
struct AlsaDevice {
    alsa: Alsa,
    handle: Pcm,
    period_size: c_ulong,
}

/// alsa_init: the device must open for playback, with libasound quiet.
fn alsa_init(params: &DeviceParams) -> Option<AlsaDevice> {
    let alsa = Alsa::load()?;
    let file = CString::new(params.file.as_deref().unwrap_or(DEFAULT_ALSA_SOUND_DEVICE)).ok()?;
    let mut handle: Pcm = std::ptr::null_mut();
    // SAFETY: the handler takes the arguments it ignores; the handle is
    // closed when the open succeeded.
    let err = unsafe {
        (alsa.lib_error_set_handler)(Some(snd_error_quiet));
        let err = (alsa.pcm_open)(&mut handle, file.as_ptr(), SND_PCM_STREAM_PLAYBACK, 0);
        (alsa.lib_error_set_handler)(None);
        if err >= 0 {
            (alsa.pcm_close)(handle);
        }
        err
    };
    (err >= 0).then(|| AlsaDevice {
        alsa,
        handle: std::ptr::null_mut(),
        period_size: 0,
    })
}

impl AlsaDevice {
    fn open(&mut self, file: &str) -> Result<(), LispError> {
        let name = CString::new(file).map_err(|_| sound_error(file.to_string()))?;
        // SAFETY: a NUL-terminated device name and a handle slot.
        let err = unsafe {
            (self.alsa.pcm_open)(&mut self.handle, name.as_ptr(), SND_PCM_STREAM_PLAYBACK, 0)
        };
        if err < 0 {
            self.handle = std::ptr::null_mut();
            return Err(self.alsa.error(file, err));
        }
        Ok(())
    }

    fn frame_bytes(&self, params: &DeviceParams) -> usize {
        // SAFETY: snd_pcm_format_size is a pure lookup.
        let fact =
            unsafe { (self.alsa.pcm_format_size)(params.format, 1) } * params.channels as isize;
        fact.max(1) as usize
    }

    fn period_size(&self, params: &DeviceParams) -> usize {
        self.period_size as usize * self.frame_bytes(params)
    }

    fn configure(&mut self, params: &DeviceParams, file: &str) -> Result<(), LispError> {
        let alsa = &self.alsa;
        let handle = self.handle;
        let mut hwparams: *mut c_void = std::ptr::null_mut();
        let mut swparams: *mut c_void = std::ptr::null_mut();
        // SAFETY: alsa_configure's calls on the open handle and the
        // parameter structures allocated here, freed on every path.
        let result = unsafe {
            (|| -> Result<(), LispError> {
                let check = |err: c_int, msg: &str| {
                    if err < 0 {
                        Err(alsa.error(msg, err))
                    } else {
                        Ok(())
                    }
                };
                check(
                    (alsa.hw_params_malloc)(&mut hwparams),
                    "Could not allocate hardware parameter structure",
                )?;
                check(
                    (alsa.sw_params_malloc)(&mut swparams),
                    "Could not allocate software parameter structure",
                )?;
                check(
                    (alsa.hw_params_any)(handle, hwparams),
                    "Could not initialize hardware parameter structure",
                )?;
                check(
                    (alsa.hw_params_set_access)(handle, hwparams, SND_PCM_ACCESS_RW_INTERLEAVED),
                    "Could not set access type",
                )?;
                check(
                    (alsa.hw_params_set_format)(handle, hwparams, params.format),
                    "Could not set sound format",
                )?;
                let mut rate = params.sample_rate as c_uint;
                check(
                    (alsa.hw_params_set_rate_near)(
                        handle,
                        hwparams,
                        &mut rate,
                        std::ptr::null_mut(),
                    ),
                    "Could not set sample rate",
                )?;
                check(
                    (alsa.hw_params_set_channels)(handle, hwparams, params.channels as c_uint),
                    "Could not set channel count",
                )?;
                check(
                    (alsa.hw_params)(handle, hwparams),
                    "Could not set parameters",
                )?;
                let mut dir = 0;
                check(
                    (alsa.hw_params_get_period_size)(hwparams, &mut self.period_size, &mut dir),
                    "Unable to get period size for playback",
                )?;
                let mut buffer_size: c_ulong = 0;
                check(
                    (alsa.hw_params_get_buffer_size)(hwparams, &mut buffer_size),
                    "Unable to get buffer size for playback",
                )?;
                check(
                    (alsa.sw_params_current)(handle, swparams),
                    "Unable to determine current swparams for playback",
                )?;
                let period = self.period_size.max(1);
                check(
                    (alsa.sw_params_set_start_threshold)(
                        handle,
                        swparams,
                        (buffer_size / period) * period,
                    ),
                    "Unable to set start threshold mode for playback",
                )?;
                check(
                    (alsa.sw_params_set_avail_min)(handle, swparams, self.period_size),
                    "Unable to set avail min for playback",
                )?;
                check(
                    (alsa.sw_params)(handle, swparams),
                    "Unable to set sw params for playback\n",
                )?;
                Ok(())
            })()
        };
        // SAFETY: each structure is freed once, when it was allocated.
        unsafe {
            if !hwparams.is_null() {
                (alsa.hw_params_free)(hwparams);
            }
            if !swparams.is_null() {
                (alsa.sw_params_free)(swparams);
            }
        }
        result?;
        // SAFETY: the configured handle.
        let err = unsafe { (alsa.pcm_prepare)(handle) };
        if err < 0 {
            return Err(alsa.error("Could not prepare audio interface for use", err));
        }
        if params.volume > 0 {
            self.set_volume(params.volume, file);
        }
        Ok(())
    }

    /// alsa_configure's mixer pass: every element with a playback volume
    /// set to VOLUME percent of its range on each channel.
    fn set_volume(&self, volume: c_int, file: &str) {
        let alsa = &self.alsa;
        let Ok(name) = CString::new(file) else {
            return;
        };
        // SAFETY: the mixer is opened, walked and closed here.
        unsafe {
            let mut mixer: *mut c_void = std::ptr::null_mut();
            if (alsa.mixer_open)(&mut mixer, 0) < 0 {
                return;
            }
            if (alsa.mixer_attach)(mixer, name.as_ptr()) >= 0
                && (alsa.mixer_load)(mixer) >= 0
                && (alsa.mixer_selem_register)(mixer, std::ptr::null_mut(), std::ptr::null_mut())
                    >= 0
            {
                let mut element = (alsa.mixer_first_elem)(mixer);
                while !element.is_null() {
                    if (alsa.mixer_selem_has_playback_volume)(element) != 0 {
                        let (mut min, mut max) = (0, 0);
                        (alsa.mixer_selem_get_playback_volume_range)(element, &mut min, &mut max);
                        let level = min + (c_long::from(volume) * (max - min)) / 100;
                        for channel in 0..=SND_MIXER_SCHN_LAST {
                            (alsa.mixer_selem_set_playback_volume)(element, channel, level);
                        }
                    }
                    element = (alsa.mixer_elem_next)(element);
                }
            }
            (alsa.mixer_close)(mixer);
        }
    }

    fn write(&mut self, params: &DeviceParams, buffer: &[u8]) -> Result<(), LispError> {
        let alsa = &self.alsa;
        let fact = self.frame_bytes(params);
        let mut written = 0;
        while written < buffer.len() {
            let frames = (buffer.len() - written) / fact;
            if frames == 0 {
                break;
            }
            // SAFETY: FRAMES whole frames lie in the rest of the buffer.
            let err = unsafe {
                (alsa.pcm_writei)(
                    self.handle,
                    buffer[written..].as_ptr().cast(),
                    frames as c_ulong,
                )
            };
            if err >= 0 {
                written += err as usize * fact;
                continue;
            }
            let err = err as c_int;
            if err == -libc::EPIPE {
                // SAFETY: the open handle.
                let err = unsafe { (alsa.pcm_prepare)(self.handle) };
                if err < 0 {
                    return Err(alsa.error("Can't recover from underrun, prepare failed", err));
                }
            } else if err == -libc::ESTRPIPE {
                // SAFETY: the open handle.
                let mut err = unsafe { (alsa.pcm_resume)(self.handle) };
                while err == -libc::EAGAIN {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    // SAFETY: the open handle.
                    err = unsafe { (alsa.pcm_resume)(self.handle) };
                }
                if err < 0 {
                    // SAFETY: the open handle.
                    let err = unsafe { (alsa.pcm_prepare)(self.handle) };
                    if err < 0 {
                        return Err(alsa.error("Can't recover from suspend, prepare failed", err));
                    }
                }
            } else {
                return Err(alsa.error("Error writing to sound device", err));
            }
        }
        Ok(())
    }

    /// alsa_close: drain and close the handle when open.
    fn close(&mut self) {
        if !self.handle.is_null() {
            // SAFETY: the open handle, closed once.
            unsafe {
                (self.alsa.pcm_drain)(self.handle);
                (self.alsa.pcm_close)(self.handle);
            }
            self.handle = std::ptr::null_mut();
        }
    }
}

/// wav_play and au_play: configure the device for the file, then copy its
/// data from the header's end (the data offset for AU).
fn play(
    sound: &SoundType,
    source: &mut SoundSource,
    device: &mut SoundDevice,
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    let (offset, limit) = match sound {
        SoundType::Riff(header) => {
            device.choose_format(sound)?;
            device.params.sample_size = c_int::from(header.sample_size);
            device.params.sample_rate = header.sample_rate as c_int;
            device.params.bps = header.bytes_per_second as c_int;
            device.params.channels = c_int::from(header.channels);
            device.configure(interp, env)?;
            (WAV_HEADER_BYTES as u64, Some(header.data_length as usize))
        }
        SoundType::SunAudio(header) => {
            device.params.sample_size = 0;
            device.params.sample_rate = header.sample_rate as c_int;
            device.params.bps = 0;
            device.params.channels = header.channels as c_int;
            device.choose_format(sound)?;
            device.configure(interp, env)?;
            (u64::from(header.data_offset), None)
        }
    };
    match source {
        SoundSource::Data(data) => {
            let start = (offset as usize).min(data.len());
            device.write(&data[start..])
        }
        SoundSource::File(file) => {
            let block = device.period_size();
            let mut buffer = vec![0; block];
            let mut left = limit;
            file.seek(SeekFrom::Start(offset)).map_err(|error| {
                sound_perror(
                    "Error reading sound file",
                    error.raw_os_error().unwrap_or(0),
                )
            })?;
            while left != Some(0) {
                let read = match file.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => read,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        return Err(sound_perror(
                            "Error reading sound file",
                            error.raw_os_error().unwrap_or(0),
                        ));
                    }
                };
                // WAV: no garbage past the data length.
                let take = left.map_or(read, |left| read.min(left));
                if let Some(left) = &mut left {
                    *left -= take;
                }
                device.write(&buffer[..take])?;
            }
            Ok(())
        }
    }
}

/// sound.c:Fplay_sound_internal.
pub(crate) fn play_sound_internal(
    interp: &mut Interpreter,
    sound: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some(attrs) = parse_sound(sound)? else {
        return Err(sound_error("Invalid sound specification"));
    };
    let (mut source, header) = if attrs.file.is_string() {
        let data_directory = interp
            .lookup_var("data-directory", env)
            .unwrap_or(Value::Nil);
        let path = Value::list([data_directory]);
        let mut file = super::loading::openp_file(interp, &attrs.file, &path, env)?
            .map_err(|errno| sound_perror("Could not open sound file", errno))?;
        let mut header = vec![0; MAX_SOUND_HEADER_BYTES];
        let mut size = 0;
        while size < header.len() {
            match file.read(&mut header[size..]) {
                Ok(0) => break,
                Ok(read) => size += read,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => {
                    return Err(sound_perror(
                        "Invalid sound file header",
                        error.raw_os_error().unwrap_or(0),
                    ));
                }
            }
        }
        header.truncate(size);
        (SoundSource::File(file), header)
    } else {
        // SDATA: the string's bytes as stored.
        let bytes = match attrs.data.kind() {
            Kind::StringObject(state) => state.borrow().bytes().to_vec(),
            _ => return Err(wrong_type_argument("stringp", attrs.data)),
        };
        let header = bytes[..bytes.len().min(MAX_SOUND_HEADER_BYTES)].to_vec();
        (SoundSource::Data(bytes), header)
    };
    let sound_type = find_sound_type(&header)?;
    let volume = match attrs.volume.kind() {
        Kind::Integer(volume) => volume as c_int,
        Kind::Float(volume) => (volume.get() * 100.0) as c_int,
        _ => 0,
    };
    let params = DeviceParams {
        file: attrs.device,
        format: 0,
        volume,
        sample_size: 0,
        sample_rate: 0,
        bps: 0,
        channels: 0,
    };
    super::call(
        interp,
        "run-hook-with-args",
        &[Value::symbol("play-sound-functions"), sound],
        env,
    )?;
    let driver = match alsa_init(&params) {
        Some(device) => Driver::Alsa(Box::new(device)),
        None if vox_init(&params) => Driver::Vox(None),
        None => return Err(sound_error("No usable sound device driver found")),
    };
    let mut device = SoundDevice { params, driver };
    device.open()?;
    play(&sound_type, &mut source, &mut device, interp, env)?;
    Ok(Value::Nil)
}
