use super::*;
use crate::lisp::types::Kind;

/// xfaces.c parse_color_spec: the numeric X color forms only -- "#RGB" with
/// hex components of equal length (1-4 digits), "rgb:R/G/B" with 1-4 hex
/// digits each, and "rgbi:R/G/B" with decimal floats in [0,1].  Names are
/// not parsed here; xfaces.c leaves them to the terminal's color lookup.
pub(crate) fn parse_color_spec(spec: &str) -> Option<[u16; 3]> {
    if let Some(rest) = spec.strip_prefix('#') {
        if rest.len() % 3 != 0 {
            return None;
        }
        let digits = rest.len() / 3;
        let bytes = rest.as_bytes();
        return Some([
            parse_hex_color_comp(&bytes[..digits])?,
            parse_hex_color_comp(&bytes[digits..2 * digits])?,
            parse_hex_color_comp(&bytes[2 * digits..])?,
        ]);
    }

    if let Some(rest) = spec.strip_prefix("rgb:") {
        let (red, rest) = rest.split_once('/')?;
        let (green, blue) = rest.split_once('/')?;
        return Some([
            parse_hex_color_comp(red.as_bytes())?,
            parse_hex_color_comp(green.as_bytes())?,
            parse_hex_color_comp(blue.as_bytes())?,
        ]);
    }

    if let Some(rest) = spec.strip_prefix("rgbi:") {
        let (red, rest) = rest.split_once('/')?;
        let (green, blue) = rest.split_once('/')?;
        let component = |text: &str| {
            let red = parse_float_color_comp(text)?;
            Some((red * 65535.0).round_ties_even() as u16)
        };
        return Some([component(red)?, component(green)?, component(blue)?]);
    }

    None
}

/// parse_hex_color_comp: 1-4 hex digits scaled to 0..65535 by integer
/// division against the digit count's maximum.
fn parse_hex_color_comp(text: &[u8]) -> Option<u16> {
    if text.is_empty() || text.len() > 4 {
        return None;
    }
    let mut value = 0u32;
    for byte in text {
        let digit = (*byte as char).to_digit(16)?;
        value = (value << 4) | digit;
    }
    let max_value = (1u32 << (text.len() * 4)) - 1;
    Some((value * 65535 / max_value) as u16)
}

/// parse_float_color_comp: a decimal literal made only of digits, `.',
/// `+', `-', `e' and `E', fully consumed by `strtod', in [0,1].
fn parse_float_color_comp(text: &str) -> Option<f64> {
    if text.is_empty()
        || !text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'.' | b'+' | b'-' | b'e' | b'E'))
    {
        return None;
    }
    let value = text.parse::<f64>().ok()?;
    (0.0..=1.0).contains(&value).then_some(value)
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Cam02Jab {
    j: f64,
    a: f64,
    b: f64,
}

define_dispatch!(
    #[inline(never)]
    pub(crate) fn call_lcms_builtin(name: &str, args: &[Value]) -> Result<Value, LispError> {
        match name {
            "lcms-cie-de2000" => {
                need_arg_range(name, args, 2, 5)?;
                let left = parse_lcms_lab_list(&args[0])
                    .ok_or_else(|| LispError::Signal("Invalid color".into()))?;
                let right = parse_lcms_lab_list(&args[1])
                    .ok_or_else(|| LispError::Signal("Invalid color".into()))?;
                let k_l = optional_lcms_number_arg(args.get(2))?;
                let k_c = optional_lcms_number_arg(args.get(3))?;
                let k_h = optional_lcms_number_arg(args.get(4))?;
                Ok(Value::float(left.cie2000_delta_e(&right, k_l, k_c, k_h)))
            }
            "lcms-xyz->jch" => {
                need_arg_range(name, args, 1, 3)?;
                let xyz = parse_lcms_xyz_list(&args[0])
                    .ok_or_else(|| LispError::Signal("Invalid color".into()))?;
                let (mut model, _) = lcms_cam02_model(args.get(1), args.get(2))?;
                Ok(lcms_jch_value(model.forward(&xyz)))
            }
            "lcms-jch->xyz" => {
                need_arg_range(name, args, 1, 3)?;
                let jch = parse_lcms_jch_list(&args[0])
                    .ok_or_else(|| LispError::Signal("Invalid color".into()))?;
                let (mut model, _) = lcms_cam02_model(args.get(1), args.get(2))?;
                Ok(lcms_scaled_xyz_value(model.reverse(&jch)))
            }
            "lcms-jch->jab" => {
                need_arg_range(name, args, 1, 3)?;
                let jch = parse_lcms_jch_list(&args[0])
                    .ok_or_else(|| LispError::Signal("Invalid color".into()))?;
                let (_, view) = lcms_cam02_model(args.get(1), args.get(2))?;
                Ok(lcms_jab_value(lcms_jch_to_jab(jch, lcms_fl(view.La))))
            }
            "lcms-jab->jch" => {
                need_arg_range(name, args, 1, 3)?;
                let jab = parse_lcms_jab_list(&args[0])
                    .ok_or_else(|| LispError::Signal("Invalid color".into()))?;
                let (_, view) = lcms_cam02_model(args.get(1), args.get(2))?;
                Ok(lcms_jch_value(lcms_jab_to_jch(jab, lcms_fl(view.La))))
            }
            "lcms-cam02-ucs" => {
                need_arg_range(name, args, 2, 4)?;
                let left = parse_lcms_xyz_list(&args[0])
                    .ok_or_else(|| LispError::Signal("Invalid color".into()))?;
                let right = parse_lcms_xyz_list(&args[1])
                    .ok_or_else(|| LispError::Signal("Invalid color".into()))?;
                let (mut model, view) = lcms_cam02_model(args.get(2), args.get(3))?;
                let fl = lcms_fl(view.La);
                let left = lcms_jch_to_jab(model.forward(&left), fl);
                let right = lcms_jch_to_jab(model.forward(&right), fl);
                Ok(Value::float(
                    (right.j - left.j).hypot((right.a - left.a).hypot(right.b - left.b)),
                ))
            }
            "lcms2-available-p" => Ok(Value::T),
            "lcms-temp->white-point" => {
                need_args(name, args, 1)?;
                let temperature = args[0].as_float()?;
                let white_point = lcms2::white_point_from_temp(temperature)
                    .ok_or_else(|| LispError::Signal("Invalid temperature".into()))?;
                Ok(lcms_white_point_value(lcms2::xyY2XYZ(&white_point)))
            }
        }
    }
);

pub(crate) fn optional_lcms_number_arg(value: Option<&Value>) -> Result<f64, LispError> {
    match value.map(|v| v.kind()) {
        None | Some(Kind::Nil) => Ok(1.0),
        Some(value) => value.value().as_float(),
    }
}

pub(crate) fn lcms_default_white_point() -> CIEXYZ {
    CIEXYZ {
        X: 95.0455,
        Y: 100.0,
        Z: 108.8753,
    }
}

pub(crate) fn lcms_default_viewing_conditions(white_point: CIEXYZ) -> ViewingConditions {
    ViewingConditions {
        whitePoint: white_point,
        Yb: 20.0,
        La: 100.0,
        surround: Surround::Avg,
        D_value: 1.0,
    }
}

pub(crate) fn parse_lcms_numeric_prefix<const N: usize>(value: &Value) -> Option<[f64; N]> {
    let mut result = [0.0; N];
    let mut current = *value;
    for item in &mut result {
        let (car, cdr) = current.cons_cells()?;
        *item = car.get().as_float().ok()?;
        current = cdr.get();
    }
    Some(result)
}

pub(crate) fn parse_lcms_xyz_list(value: &Value) -> Option<CIEXYZ> {
    let [x, y, z] = parse_lcms_numeric_prefix::<3>(value)?;
    Some(CIEXYZ {
        X: x * 100.0,
        Y: y * 100.0,
        Z: z * 100.0,
    })
}

pub(crate) fn parse_lcms_lab_list(value: &Value) -> Option<CIELab> {
    let [l, a, b] = parse_lcms_numeric_prefix::<3>(value)?;
    Some(CIELab { L: l, a, b })
}

pub(crate) fn parse_lcms_jab_list(value: &Value) -> Option<Cam02Jab> {
    let [j, a, b] = parse_lcms_numeric_prefix::<3>(value)?;
    Some(Cam02Jab { j, a, b })
}

pub(crate) fn parse_lcms_jch_list(value: &Value) -> Option<JCh> {
    let items = value.to_vec().ok()?;
    if items.len() != 3 {
        return None;
    }
    Some(JCh {
        J: items[0].as_float().ok()?,
        C: items[1].as_float().ok()?,
        h: items[2].as_float().ok()?,
    })
}

pub(crate) fn parse_lcms_viewing_conditions(
    value: &Value,
    white_point: CIEXYZ,
) -> Option<ViewingConditions> {
    let items = value.to_vec().ok()?;
    if items.len() != 4 {
        return None;
    }
    Some(ViewingConditions {
        whitePoint: white_point,
        Yb: items[0].as_float().ok()?,
        La: items[1].as_float().ok()?,
        surround: match items[2].integer_value()? {
            1 => Surround::Avg,
            2 => Surround::Dim,
            3 => Surround::Dark,
            4 => Surround::Cutsheet,
            _ => return None,
        },
        D_value: items[3].as_float().ok()?,
    })
}

pub(crate) fn lcms_cam02_model(
    white_point: Option<&Value>,
    view: Option<&Value>,
) -> Result<(CIECAM02, ViewingConditions), LispError> {
    let white_point = match white_point.map(|v| v.kind()) {
        None | Some(Kind::Nil) => lcms_default_white_point(),
        Some(value) => parse_lcms_xyz_list(&value.value())
            .ok_or_else(|| LispError::Signal("Invalid white point".into()))?,
    };
    let viewing_conditions = match view.map(|v| v.kind()) {
        None | Some(Kind::Nil) => lcms_default_viewing_conditions(white_point),
        Some(value) => parse_lcms_viewing_conditions(&value.value(), white_point)
            .ok_or_else(|| LispError::Signal("Invalid view conditions".into()))?,
    };
    let model = CIECAM02::new(viewing_conditions)
        .map_err(|error| LispError::Signal(format!("lcms2 model init failed: {error}")))?;
    Ok((model, viewing_conditions))
}

pub(crate) fn lcms_fl(la: f64) -> f64 {
    let k = 1.0 / (1.0 + (5.0 * la));
    let k4 = k.powi(4);
    la * k4 + 0.1 * (1.0 - k4).powi(2) * (5.0 * la).cbrt()
}

pub(crate) fn lcms_jch_to_jab(jch: JCh, fl: f64) -> Cam02Jab {
    let m_prime = 43.86 * (1.0 + (0.0228 * (jch.C * fl.sqrt().sqrt()))).ln();
    let hue = jch.h.to_radians();
    Cam02Jab {
        j: 1.7 * jch.J / (1.0 + (0.007 * jch.J)),
        a: m_prime * hue.cos(),
        b: m_prime * hue.sin(),
    }
}

pub(crate) fn lcms_jab_to_jch(jab: Cam02Jab, fl: f64) -> JCh {
    let m_prime = jab.a.hypot(jab.b);
    let mut hue = jab.b.atan2(jab.a).to_degrees();
    if hue < 0.0 {
        hue += 360.0;
    }
    JCh {
        J: jab.j / (1.0 + (0.007 * (100.0 - jab.j))),
        C: ((0.0228 * m_prime).exp() - 1.0) / fl.sqrt().sqrt() / 0.0228,
        h: hue,
    }
}

pub(crate) fn lcms_jch_value(jch: JCh) -> Value {
    Value::list([
        Value::float(jch.J),
        Value::float(jch.C),
        Value::float(jch.h),
    ])
}

pub(crate) fn lcms_jab_value(jab: Cam02Jab) -> Value {
    Value::list([
        Value::float(jab.j),
        Value::float(jab.a),
        Value::float(jab.b),
    ])
}

pub(crate) fn lcms_scaled_xyz_value(xyz: CIEXYZ) -> Value {
    Value::list([
        Value::float(xyz.X / 100.0),
        Value::float(xyz.Y / 100.0),
        Value::float(xyz.Z / 100.0),
    ])
}

pub(crate) fn lcms_white_point_value(xyz: CIEXYZ) -> Value {
    Value::list([
        Value::float(xyz.X),
        Value::float(xyz.Y),
        Value::float(xyz.Z),
    ])
}
