use super::*;

pub(crate) fn parse_color_spec(spec: &str) -> Option<[u16; 3]> {
    if let Some(values) = named_color_spec(spec) {
        return Some(values);
    }

    if let Some(rest) = spec.strip_prefix('#') {
        if rest.is_empty() || rest.len() % 3 != 0 {
            return None;
        }
        let digits = rest.len() / 3;
        if !(1..=4).contains(&digits) {
            return None;
        }
        let mut values = [0u16; 3];
        for (index, chunk) in rest.as_bytes().chunks(digits).enumerate() {
            let text = std::str::from_utf8(chunk).ok()?;
            if !text.chars().all(|ch| ch.is_ascii_hexdigit()) {
                return None;
            }
            values[index] = expand_hex_component(text)?;
        }
        return Some(values);
    }

    if let Some(rest) = spec.strip_prefix("rgb:") {
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return None;
        }
        let mut values = [0u16; 3];
        for (index, part) in parts.iter().enumerate() {
            if !part.chars().all(|ch| ch.is_ascii_hexdigit()) || part.len() > 4 {
                return None;
            }
            values[index] = expand_hex_component(part)?;
        }
        return Some(values);
    }

    if let Some(rest) = spec.strip_prefix("rgbi:") {
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return None;
        }
        let mut values = [0u16; 3];
        for (index, part) in parts.iter().enumerate() {
            if part.chars().any(char::is_whitespace) {
                return None;
            }
            let value = part.parse::<f64>().ok()?;
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return None;
            }
            values[index] = (value * 65535.0).round() as u16;
        }
        return Some(values);
    }

    None
}

fn named_color_spec(spec: &str) -> Option<[u16; 3]> {
    match spec.to_ascii_lowercase().as_str() {
        "black" => Some([0, 0, 0]),
        "white" => Some([0xffff, 0xffff, 0xffff]),
        "red" => Some([0xffff, 0, 0]),
        "green" => Some([0, 0xffff, 0]),
        "blue" => Some([0, 0, 0xffff]),
        _ => None,
    }
}

pub(crate) fn expand_hex_component(component: &str) -> Option<u16> {
    if component.is_empty() || component.len() > 4 {
        return None;
    }
    let value = u32::from_str_radix(component, 16).ok()?;
    let bits = 4 * component.len();
    let max_value = (1u32 << bits) - 1;
    Some(((value * 0xFFFF) / max_value) as u16)
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
                Ok(Value::Float(left.cie2000_delta_e(&right, k_l, k_c, k_h)))
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
                Ok(Value::Float(
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
    match value {
        None | Some(Value::Nil) => Ok(1.0),
        Some(value) => value.as_float(),
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
    let mut current = value.clone();
    for item in &mut result {
        let Value::Cons(car, cdr) = current else {
            return None;
        };
        *item = car.borrow().as_float().ok()?;
        current = cdr.borrow().clone();
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
        surround: match items[2].as_integer().ok()? {
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
    let white_point = match white_point {
        None | Some(Value::Nil) => lcms_default_white_point(),
        Some(value) => parse_lcms_xyz_list(value)
            .ok_or_else(|| LispError::Signal("Invalid white point".into()))?,
    };
    let viewing_conditions = match view {
        None | Some(Value::Nil) => lcms_default_viewing_conditions(white_point),
        Some(value) => parse_lcms_viewing_conditions(value, white_point)
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
        Value::Float(jch.J),
        Value::Float(jch.C),
        Value::Float(jch.h),
    ])
}

pub(crate) fn lcms_jab_value(jab: Cam02Jab) -> Value {
    Value::list([
        Value::Float(jab.j),
        Value::Float(jab.a),
        Value::Float(jab.b),
    ])
}

pub(crate) fn lcms_scaled_xyz_value(xyz: CIEXYZ) -> Value {
    Value::list([
        Value::Float(xyz.X / 100.0),
        Value::Float(xyz.Y / 100.0),
        Value::Float(xyz.Z / 100.0),
    ])
}

pub(crate) fn lcms_white_point_value(xyz: CIEXYZ) -> Value {
    Value::list([
        Value::Float(xyz.X),
        Value::Float(xyz.Y),
        Value::Float(xyz.Z),
    ])
}

#[derive(Default)]
pub(crate) struct FontSpecInfo {
    pub(crate) family: Option<String>,
    pub(crate) size: Option<f64>,
    pub(crate) weight: Option<String>,
    pub(crate) slant: Option<String>,
    pub(crate) spacing: Option<i64>,
    pub(crate) foundry: Option<String>,
}

pub(crate) fn parse_font_name(name: &str) -> FontSpecInfo {
    if name.starts_with('-') {
        return parse_xlfd_font_name(name);
    }
    if name.chars().next().is_some_and(char::is_whitespace)
        || name.chars().last().is_some_and(char::is_whitespace)
    {
        return FontSpecInfo {
            family: Some(name.to_string()),
            ..FontSpecInfo::default()
        };
    }
    if name.contains(':')
        || name
            .rsplit_once('-')
            .is_some_and(|(family, size)| !family.is_empty() && size.parse::<f64>().is_ok())
    {
        return parse_fontconfig_name(name);
    }
    parse_gtk_font_name(name)
}

pub(crate) fn parse_xlfd_font_name(name: &str) -> FontSpecInfo {
    let mut info = FontSpecInfo::default();
    let parts = name.split('-').skip(1).collect::<Vec<_>>();
    if parts.len() < 3 {
        return info;
    }
    info.foundry = parts.first().map(|part| (*part).to_string());
    let weight_index = parts
        .iter()
        .enumerate()
        .skip(2)
        .find(|(index, part)| {
            is_weight_name(part)
                && parts
                    .get(index + 1)
                    .is_some_and(|next| is_slant_name(next) || is_width_name(next))
        })
        .map(|(index, _)| index)
        .unwrap_or(2);
    if weight_index > 1 {
        info.family = Some(parts[1..weight_index].join("-"));
    }
    info.weight = parts
        .get(weight_index)
        .and_then(|part| normalize_weight(part));
    info
}

pub(crate) fn parse_fontconfig_name(name: &str) -> FontSpecInfo {
    let mut info = FontSpecInfo::default();
    let mut sections = name.split(':');
    let base = sections.next().unwrap_or_default();
    parse_family_and_size_segment(base, &mut info);
    for section in sections {
        apply_font_attr(section, &mut info);
    }
    info
}

pub(crate) fn parse_gtk_font_name(name: &str) -> FontSpecInfo {
    let mut info = FontSpecInfo::default();
    let mut tokens = name
        .split_whitespace()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        if !name.is_empty() {
            info.family = Some(name.to_string());
        }
        return info;
    }
    if tokens
        .last()
        .is_some_and(|token| token.parse::<f64>().is_ok())
    {
        info.size = tokens.pop().and_then(|token| token.parse::<f64>().ok());
    }
    while let Some(token) = tokens.last().cloned() {
        if let Some(weight) = normalize_weight(&token) {
            if info.weight.is_none() {
                info.weight = Some(weight);
            }
            tokens.pop();
            continue;
        }
        if let Some(slant) = normalize_slant(&token) {
            if info.slant.is_none() {
                info.slant = Some(slant);
            }
            tokens.pop();
            continue;
        }
        if let Some(spacing) = normalize_spacing(&token) {
            if info.spacing.is_none() {
                info.spacing = Some(spacing);
            }
            tokens.pop();
            continue;
        }
        if is_width_name(&token) {
            tokens.pop();
            continue;
        }
        break;
    }
    if !tokens.is_empty() {
        info.family = Some(tokens.join(" "));
    }
    info
}

pub(crate) fn parse_family_and_size_segment(base: &str, info: &mut FontSpecInfo) {
    if base.parse::<f64>().is_ok() {
        info.size = base.parse::<f64>().ok();
        return;
    }
    if let Some((family, size)) = base.rsplit_once('-')
        && !family.is_empty()
        && size.parse::<f64>().is_ok()
    {
        info.family = Some(family.to_string());
        info.size = size.parse::<f64>().ok();
        return;
    }
    if !base.is_empty() {
        info.family = Some(base.to_string());
    }
}

pub(crate) fn apply_font_attr(section: &str, info: &mut FontSpecInfo) {
    if let Some((key, value)) = section.split_once('=') {
        match key {
            "weight" => info.weight = normalize_weight(value),
            "slant" => info.slant = normalize_slant(value),
            _ => {}
        }
        return;
    }
    if let Some(weight) = normalize_weight(section) {
        info.weight = Some(weight);
    } else if let Some(slant) = normalize_slant(section) {
        info.slant = Some(slant);
    } else if let Some(spacing) = normalize_spacing(section) {
        info.spacing = Some(spacing);
    }
}

pub(crate) fn normalize_weight(token: &str) -> Option<String> {
    match token.to_ascii_lowercase().as_str() {
        "ultra-light" => Some("ultra-light".into()),
        "light" => Some("light".into()),
        "book" => Some("book".into()),
        "medium" => Some("medium".into()),
        "demibold" => Some("demibold".into()),
        "semi-bold" | "semibold" => Some("semi-bold".into()),
        "bold" => Some("bold".into()),
        "black" => Some("black".into()),
        "normal" => Some("normal".into()),
        _ => None,
    }
}

pub(crate) fn normalize_slant(token: &str) -> Option<String> {
    match token.to_ascii_lowercase().as_str() {
        "italic" => Some("italic".into()),
        "oblique" => Some("oblique".into()),
        "roman" => Some("roman".into()),
        "normal" => Some("normal".into()),
        _ => None,
    }
}

pub(crate) fn normalize_spacing(token: &str) -> Option<i64> {
    match token.to_ascii_lowercase().as_str() {
        "mono" => Some(100),
        "proportional" => Some(0),
        _ => None,
    }
}

pub(crate) fn is_weight_name(token: &str) -> bool {
    normalize_weight(token).is_some()
}

pub(crate) fn is_slant_name(token: &str) -> bool {
    normalize_slant(token).is_some()
}

pub(crate) fn is_width_name(token: &str) -> bool {
    matches!(
        token.to_ascii_lowercase().as_str(),
        "normal" | "semi-condensed"
    )
}
