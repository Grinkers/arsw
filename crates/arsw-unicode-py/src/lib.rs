use pyo3::exceptions::{PyException, PyIndexError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyByteArray, PyBytes, PyFrozenSet, PyModule, PyString};

const UNICODE_VERSION: &str = "17.0";
const HARD_BREAKS: [u32; 7] = [0x000A, 0x000B, 0x000C, 0x000D, 0x0085, 0x2028, 0x2029];

const CAT_CC: u64 = 1 << 0;
const CAT_CF: u64 = 1 << 1;
const CAT_CN: u64 = 1 << 2;
const CAT_CO: u64 = 1 << 3;
const CAT_CS: u64 = 1 << 4;
const CAT_EXTENDED_PICTOGRAPHIC: u64 = 1 << 5;
const CAT_LL: u64 = 1 << 6;
const CAT_LM: u64 = 1 << 7;
const CAT_LO: u64 = 1 << 8;
const CAT_LT: u64 = 1 << 9;
const CAT_LU: u64 = 1 << 10;
const CAT_MC: u64 = 1 << 11;
const CAT_ME: u64 = 1 << 12;
const CAT_MN: u64 = 1 << 13;
const CAT_ND: u64 = 1 << 14;
const CAT_NL: u64 = 1 << 15;
const CAT_NO: u64 = 1 << 16;
const CAT_PC: u64 = 1 << 17;
const CAT_PD: u64 = 1 << 18;
const CAT_PE: u64 = 1 << 19;
const CAT_PF: u64 = 1 << 20;
const CAT_PI: u64 = 1 << 21;
const CAT_PO: u64 = 1 << 22;
const CAT_PS: u64 = 1 << 23;
const CAT_REGIONAL_INDICATOR: u64 = 1 << 24;
const CAT_SC: u64 = 1 << 25;
const CAT_SK: u64 = 1 << 26;
const CAT_SM: u64 = 1 << 27;
const CAT_SO: u64 = 1 << 28;
const CAT_WIDTH_INVALID: u64 = 1 << 29;
const CAT_WIDTH_TWO: u64 = 1 << 30;
const CAT_WIDTH_ZERO: u64 = 1 << 31;
const CAT_ZL: u64 = 1 << 32;
const CAT_ZP: u64 = 1 << 33;
const CAT_ZS: u64 = 1 << 34;

#[pyclass(name = "to_utf8_position_mapper", module = "apsw._unicode")]
struct ToUtf8PositionMapper {
	text: String,
	map: Vec<usize>,
}

#[pymethods]
impl ToUtf8PositionMapper {
	#[new]
	fn new(utf8: &Bound<'_, PyAny>) -> PyResult<Self> {
		if utf8.is_instance_of::<PyString>() {
			return Err(PyTypeError::new_err("Expected Buffer compatible, not str"));
		}

		let raw: Vec<u8> =
			utf8.extract().map_err(|_| PyTypeError::new_err("Expected Buffer compatible"))?;
		let text = std::str::from_utf8(&raw)
			.map_err(|e| PyValueError::new_err(format!("invalid utf8 bytes: {e}")))?
			.to_owned();

		let mut map = Vec::new();
		for (i, _) in text.char_indices() {
			map.push(i);
		}
		map.push(text.len());

		Ok(Self { text, map })
	}

	fn __call__(&self, pos: isize) -> PyResult<usize> {
		if pos < 0 {
			return Err(PyValueError::new_err("position needs to be zero or positive"));
		}

		let idx = usize::try_from(pos)
			.map_err(|_| PyValueError::new_err("position needs to be zero or positive"))?;

		self
			.map
			.get(idx)
			.copied()
			.ok_or_else(|| PyIndexError::new_err("position is beyond end of string"))
	}

	#[getter]
	#[pyo3(name = "str")]
	fn str_(&self) -> String {
		self.text.clone()
	}
}

#[pyclass(name = "from_utf8_position_mapper", module = "apsw._unicode")]
struct FromUtf8PositionMapper {
	bytes: Vec<u8>,
	map: Vec<Option<usize>>,
}

#[pymethods]
impl FromUtf8PositionMapper {
	#[new]
	fn new(string: &Bound<'_, PyAny>) -> PyResult<Self> {
		if string.is_instance_of::<PyBytes>() || string.is_instance_of::<PyByteArray>() {
			return Err(PyTypeError::new_err("Expected a str not bytes"));
		}

		let string: String = string.extract().map_err(|_| PyTypeError::new_err("Expected a str"))?;
		let bytes = string.as_bytes().to_vec();
		let mut map = vec![None; bytes.len() + 1];
		let mut codepoint = 0usize;
		for (byte_offset, _) in string.char_indices() {
			map[byte_offset] = Some(codepoint);
			codepoint += 1;
		}
		map[bytes.len()] = Some(codepoint);
		Ok(Self { bytes, map })
	}

	fn __call__(&self, pos: isize) -> PyResult<usize> {
		if pos < 0 {
			return Err(PyValueError::new_err("position needs to be zero to length of utf8"));
		}

		let idx = usize::try_from(pos)
			.map_err(|_| PyValueError::new_err("position needs to be zero to length of utf8"))?;

		let Some(location) = self.map.get(idx) else {
			return Err(PyIndexError::new_err("position needs to be zero to length of utf8"));
		};

		(*location).ok_or_else(|| {
			PyValueError::new_err(format!("position {idx} is an invalid offset in the utf8"))
		})
	}

	#[getter]
	fn bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
		PyBytes::new(py, &self.bytes)
	}
}

#[derive(Clone, Copy)]
struct MapperEntry {
	location: usize,
	offset: usize,
}

#[pyclass(name = "OffsetMapper", module = "apsw._unicode")]
struct OffsetMapper {
	accumulate: Option<Vec<String>>,
	text: Option<String>,
	offset_map: Vec<MapperEntry>,
	last_location: usize,
	last_offset: usize,
	length: usize,
	last_is_separator: bool,
}

#[pymethods]
impl OffsetMapper {
	#[new]
	fn new() -> Self {
		Self {
			accumulate: Some(Vec::new()),
			text: None,
			offset_map: vec![MapperEntry { location: 0, offset: 0 }],
			last_location: 0,
			last_offset: 0,
			length: 0,
			last_is_separator: false,
		}
	}

	fn add(&mut self, text: &str, source_start: isize, source_end: isize) -> PyResult<()> {
		let Some(accumulate) = self.accumulate.as_mut() else {
			return Err(PyException::new_err(
				"Text has been materialized - you cannot add more segments",
			));
		};

		if source_start < 0 || source_end < 0 {
			return Err(PyValueError::new_err("source offsets cannot be negative"));
		}

		let source_start = usize::try_from(source_start)
			.map_err(|_| PyValueError::new_err("source offsets cannot be negative"))?;
		let source_end = usize::try_from(source_end)
			.map_err(|_| PyValueError::new_err("source offsets cannot be negative"))?;

		if source_end < source_start {
			return Err(PyValueError::new_err(format!(
				"Source end {source_end} is before source start {source_start}",
			)));
		}

		let previous_end = self.offset_map[self.offset_map.len() - 1].offset;
		if source_start < previous_end {
			return Err(PyValueError::new_err(format!(
				"Source start {source_start} is before previous end {previous_end}",
			)));
		}

		accumulate.push(text.to_owned());
		self.offset_map.push(MapperEntry { location: self.length, offset: source_start });
		self.length += text.chars().count();
		self.offset_map.push(MapperEntry { location: self.length, offset: source_end });
		self.last_is_separator = false;
		Ok(())
	}

	fn separate(&mut self) -> PyResult<()> {
		let Some(accumulate) = self.accumulate.as_mut() else {
			return Err(PyException::new_err(
				"Text has been materialized - you cannot add more segments",
			));
		};

		if self.last_is_separator {
			return Ok(());
		}

		accumulate.push("\n".to_owned());
		self.length += 1;
		self.last_is_separator = true;
		Ok(())
	}

	#[getter]
	fn text(&mut self) -> String {
		if let Some(existing) = &self.text {
			return existing.clone();
		}

		let joined = self.accumulate.take().unwrap_or_default().into_iter().collect::<String>();
		self.text = Some(joined.clone());
		joined
	}

	fn __call__(&mut self, location: isize) -> PyResult<usize> {
		if self.text.is_none() {
			return Err(PyException::new_err(
				"Text has not been materialized - you cannot get offsets until getting text",
			));
		}

		if location < 0 {
			return Err(PyIndexError::new_err("location is out of range"));
		}

		let location =
			usize::try_from(location).map_err(|_| PyIndexError::new_err("location is out of range"))?;

		if location < self.last_location {
			self.last_location = 0;
			self.last_offset = 0;
		}

		for i in self.last_offset..self.offset_map.len().saturating_sub(1) {
			if location >= self.offset_map[i].location && location < self.offset_map[i + 1].location {
				self.last_location = self.offset_map[i].location;
				self.last_offset = i;
				return Ok(self.offset_map[i].offset + (location - self.last_location));
			}
		}

		let last = self.offset_map[self.offset_map.len() - 1];
		if location == last.location {
			return Ok(last.offset);
		}

		Err(PyIndexError::new_err("location is out of range"))
	}
}

fn unicode_data<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyModule>> {
	PyModule::import(py, "unicodedata")
}

fn extract_codepoint(codepoint: &Bound<'_, PyAny>) -> PyResult<char> {
	if codepoint.is_instance_of::<PyString>() {
		let text: String = codepoint.extract()?;
		let mut chars = text.chars();
		let Some(c) = chars.next() else {
			return Err(PyValueError::new_err("string codepoint cannot be empty"));
		};
		if chars.next().is_some() {
			return Err(PyTypeError::new_err("string codepoint must be one character"));
		}
		return Ok(c);
	}

	if let Ok(value) = codepoint.extract::<u32>() {
		if let Some(c) = char::from_u32(value) {
			return Ok(c);
		}
		return Err(PyValueError::new_err("codepoint is outside Unicode range"));
	}

	Err(PyTypeError::new_err("codepoint must be an int or one-character str"))
}

fn is_regional_indicator(c: char) -> bool {
	let cp = c as u32;
	(0x1F1E6..=0x1F1FF).contains(&cp)
}

fn is_variation_selector(c: char) -> bool {
	let cp = c as u32;
	(0xFE00..=0xFE0F).contains(&cp) || (0xE0100..=0xE01EF).contains(&cp)
}

fn is_emoji_modifier(c: char) -> bool {
	let cp = c as u32;
	(0x1F3FB..=0x1F3FF).contains(&cp)
}

fn is_extended_pictographic(c: char) -> bool {
	let cp = c as u32;
	((0x1F300..=0x1FAFF).contains(&cp) || (0x2600..=0x27BF).contains(&cp)) && !is_emoji_modifier(c)
}

fn char_category(py: Python<'_>, c: char) -> PyResult<String> {
	unicode_data(py)?.call_method1("category", (c.to_string(),))?.extract()
}

fn char_is_combining(py: Python<'_>, c: char) -> PyResult<bool> {
	let combining: usize =
		unicode_data(py)?.call_method1("combining", (c.to_string(),))?.extract()?;
	Ok(combining > 0)
}

fn char_is_width_two(py: Python<'_>, c: char, category: &str) -> PyResult<bool> {
	if matches!(category, "Cc" | "Cs" | "Cn" | "Co") {
		return Ok(false);
	}

	if is_emoji_modifier(c) {
		return Ok(false);
	}

	if is_extended_pictographic(c) {
		return Ok(true);
	}

	let width: String =
		unicode_data(py)?.call_method1("east_asian_width", (c.to_string(),))?.extract()?;
	Ok(width == "F" || width == "W")
}

fn category_mask_for_code(py: Python<'_>, c: char) -> PyResult<u64> {
	let cat = char_category(py, c)?;
	let mut mask = match cat.as_str() {
		"Cc" => CAT_CC,
		"Cf" => CAT_CF,
		"Cn" => CAT_CN,
		"Co" => CAT_CO,
		"Cs" => CAT_CS,
		"Ll" => CAT_LL,
		"Lm" => CAT_LM,
		"Lo" => CAT_LO,
		"Lt" => CAT_LT,
		"Lu" => CAT_LU,
		"Mc" => CAT_MC,
		"Me" => CAT_ME,
		"Mn" => CAT_MN,
		"Nd" => CAT_ND,
		"Nl" => CAT_NL,
		"No" => CAT_NO,
		"Pc" => CAT_PC,
		"Pd" => CAT_PD,
		"Pe" => CAT_PE,
		"Pf" => CAT_PF,
		"Pi" => CAT_PI,
		"Po" => CAT_PO,
		"Ps" => CAT_PS,
		"Sc" => CAT_SC,
		"Sk" => CAT_SK,
		"Sm" => CAT_SM,
		"So" => CAT_SO,
		"Zl" => CAT_ZL,
		"Zp" => CAT_ZP,
		"Zs" => CAT_ZS,
		_ => 0,
	};

	if is_regional_indicator(c) {
		mask |= CAT_REGIONAL_INDICATOR;
	}
	if is_extended_pictographic(c) {
		mask |= CAT_EXTENDED_PICTOGRAPHIC;
	}
	if char_is_combining(py, c)?
		|| is_variation_selector(c)
		|| c == '\u{200D}'
		|| is_emoji_modifier(c)
	{
		mask |= CAT_WIDTH_ZERO;
	}
	if char_is_width_two(py, c, cat.as_str())? {
		mask |= CAT_WIDTH_TWO;
	}
	if matches!(cat.as_str(), "Cc" | "Cs" | "Cn" | "Co") {
		mask |= CAT_WIDTH_INVALID;
	}

	Ok(mask)
}

fn validate_range(text: &str, start: isize, end: isize) -> PyResult<(usize, usize)> {
	let len = text.chars().count();
	if start < 0 || end < 0 {
		return Err(PyValueError::new_err("offsets must be non-negative"));
	}
	let start =
		usize::try_from(start).map_err(|_| PyValueError::new_err("offsets must be non-negative"))?;
	let end =
		usize::try_from(end).map_err(|_| PyValueError::new_err("offsets must be non-negative"))?;
	if start > end || end > len {
		return Err(PyValueError::new_err("offset range is out of bounds"));
	}
	Ok((start, end))
}

fn chars_of(text: &str) -> Vec<char> {
	text.chars().collect()
}

fn grapheme_next_break_impl(py: Python<'_>, text: &str, offset: usize) -> PyResult<usize> {
	let chars = chars_of(text);
	if offset >= chars.len() {
		return Ok(chars.len());
	}
	if chars[offset] == '\r' && offset + 1 < chars.len() && chars[offset + 1] == '\n' {
		return Ok(offset + 2);
	}

	if chars[offset] == '\u{200D}'
		|| is_variation_selector(chars[offset])
		|| char_is_combining(py, chars[offset])?
	{
		return Ok((offset + 1).min(chars.len()));
	}

	let mut index = offset + 1;
	if is_regional_indicator(chars[offset])
		&& index < chars.len()
		&& is_regional_indicator(chars[index])
	{
		return Ok(index + 1);
	}

	while index < chars.len() {
		let current = chars[index];
		let previous = chars[index - 1];
		if current == '\u{200D}'
			|| previous == '\u{200D}'
			|| is_variation_selector(current)
			|| is_emoji_modifier(current)
			|| char_is_combining(py, current)?
		{
			index += 1;
			continue;
		}
		break;
	}

	Ok(index)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BreakClass {
	Space,
	AlphaNum,
	RegionalIndicator,
	Emoji,
	Punct,
	Extend,
	Other,
}

fn break_class(py: Python<'_>, c: char) -> PyResult<BreakClass> {
	if c.is_whitespace() {
		return Ok(BreakClass::Space);
	}

	if c == '\u{200D}' || is_variation_selector(c) || char_is_combining(py, c)? {
		return Ok(BreakClass::Extend);
	}

	if is_regional_indicator(c) {
		return Ok(BreakClass::RegionalIndicator);
	}

	if is_extended_pictographic(c) || is_emoji_modifier(c) {
		return Ok(BreakClass::Emoji);
	}

	let category = char_category(py, c)?;
	if matches!(category.as_str(), "Lu" | "Ll" | "Lt" | "Lm" | "Lo" | "Nd" | "Nl" | "No") {
		return Ok(BreakClass::AlphaNum);
	}

	if category.starts_with('P') {
		return Ok(BreakClass::Punct);
	}

	Ok(BreakClass::Other)
}

fn normalize_slice_index(value: isize, len: usize) -> usize {
	if value < 0 {
		let delta = value.unsigned_abs();
		len.saturating_sub(delta)
	} else {
		usize::try_from(value).unwrap_or(usize::MAX).min(len)
	}
}

fn grapheme_boundaries(py: Python<'_>, text: &str) -> PyResult<Vec<usize>> {
	let len = text.chars().count();
	let mut boundaries = Vec::new();
	boundaries.push(0);
	let mut offset = 0usize;
	while offset < len {
		let next = grapheme_next_break_impl(py, text, offset)?;
		if next <= offset {
			break;
		}
		boundaries.push(next);
		offset = next;
	}
	if boundaries[boundaries.len() - 1] != len {
		boundaries.push(len);
	}
	Ok(boundaries)
}

#[pyfunction]
fn category_name(
	py: Python<'_>,
	_kind: &str,
	codepoint: &Bound<'_, PyAny>,
) -> PyResult<Vec<String>> {
	let c = extract_codepoint(codepoint)?;
	Ok(vec![char_category(py, c)?])
}

#[pyfunction]
fn category_category(py: Python<'_>, codepoint: &Bound<'_, PyAny>) -> PyResult<u64> {
	let c = extract_codepoint(codepoint)?;
	category_mask_for_code(py, c)
}

#[pyfunction]
fn has_category(py: Python<'_>, text: &str, start: isize, end: isize, mask: u64) -> PyResult<bool> {
	let (start, end) = validate_range(text, start, end)?;
	for (i, c) in text.chars().enumerate() {
		if i < start || i >= end {
			continue;
		}
		if category_mask_for_code(py, c)? & mask != 0 {
			return Ok(true);
		}
	}
	Ok(false)
}

#[pyfunction]
fn casefold(py: Python<'_>, text: &str) -> PyResult<String> {
	PyString::new(py, text).call_method0("casefold")?.extract()
}

#[pyfunction]
fn strip(py: Python<'_>, text: &str) -> PyResult<String> {
	let normalized: String =
		unicode_data(py)?.call_method1("normalize", ("NFKD", text))?.extract()?;

	let mut out = String::new();
	for c in normalized.chars() {
		if is_regional_indicator(c) {
			out.push(c);
			continue;
		}

		let category = char_category(py, c)?;
		if matches!(
			category.as_str(),
			"Lu" | "Ll" | "Lt" | "Lm" | "Lo" | "Nd" | "Nl" | "No" | "Sc" | "Sm" | "So"
		) {
			out.push(c);
		}
	}

	Ok(out)
}

#[pyfunction(signature = (text, offset=0))]
fn grapheme_next_break(py: Python<'_>, text: &str, offset: isize) -> PyResult<usize> {
	if offset < 0 {
		return Err(PyValueError::new_err("offset must be non-negative"));
	}
	let offset =
		usize::try_from(offset).map_err(|_| PyValueError::new_err("offset must be non-negative"))?;
	let len = text.chars().count();
	if offset > len {
		return Err(PyValueError::new_err("offset is out of bounds"));
	}
	grapheme_next_break_impl(py, text, offset)
}

#[pyfunction(signature = (text, offset=0))]
fn grapheme_length(py: Python<'_>, text: &str, offset: isize) -> PyResult<usize> {
	if offset < 0 {
		return Err(PyValueError::new_err("offset must be non-negative"));
	}
	let mut at =
		usize::try_from(offset).map_err(|_| PyValueError::new_err("offset must be non-negative"))?;
	let len = text.chars().count();
	if at > len {
		return Err(PyValueError::new_err("offset is out of bounds"));
	}

	let mut count = 0usize;
	while at < len {
		let next = grapheme_next_break_impl(py, text, at)?;
		if next <= at {
			break;
		}
		count += 1;
		at = next;
	}
	Ok(count)
}

#[pyfunction(signature = (text, start=None, stop=None))]
fn grapheme_substr(
	py: Python<'_>,
	text: &str,
	start: Option<isize>,
	stop: Option<isize>,
) -> PyResult<String> {
	let boundaries = grapheme_boundaries(py, text)?;
	let clusters = boundaries.len().saturating_sub(1);

	let start = normalize_slice_index(start.unwrap_or(0), clusters);
	let stop = normalize_slice_index(stop.unwrap_or(clusters as isize), clusters);

	if stop <= start {
		return Ok(String::new());
	}

	let from = boundaries[start];
	let to = boundaries[stop];
	Ok(text.chars().skip(from).take(to - from).collect())
}

#[pyfunction]
fn grapheme_find(
	py: Python<'_>,
	text: &str,
	substring: &str,
	start: isize,
	end: isize,
) -> PyResult<isize> {
	if substring.is_empty() {
		return Ok(-1);
	}

	let chars = chars_of(text);
	let needle = chars_of(substring);
	let len = chars.len();

	let start = normalize_slice_index(start, len);
	let end = normalize_slice_index(end, len);
	if start >= end || needle.len() > end.saturating_sub(start) {
		return Ok(-1);
	}

	let boundaries = grapheme_boundaries(py, text)?;
	let mut is_boundary = vec![false; len + 1];
	for b in boundaries {
		is_boundary[b] = true;
	}

	for idx in start..=(end - needle.len()) {
		if chars[idx..idx + needle.len()] == needle[..]
			&& is_boundary[idx]
			&& is_boundary[idx + needle.len()]
		{
			return Ok(idx as isize);
		}
	}

	Ok(-1)
}

#[pyfunction(signature = (text, offset=0))]
fn text_width(py: Python<'_>, text: &str, offset: isize) -> PyResult<isize> {
	if offset < 0 {
		return Err(PyValueError::new_err("offset must be non-negative"));
	}
	let mut at =
		usize::try_from(offset).map_err(|_| PyValueError::new_err("offset must be non-negative"))?;
	let len = text.chars().count();
	if at > len {
		return Err(PyValueError::new_err("offset is out of bounds"));
	}

	let chars = chars_of(text);
	let mut width = 0isize;
	let mut last_was_zwj = false;

	while at < len {
		let c = chars[at];
		let cat = category_mask_for_code(py, c)?;
		if (cat & CAT_WIDTH_INVALID) != 0 {
			return Ok(-1);
		}

		if last_was_zwj && (cat & CAT_EXTENDED_PICTOGRAPHIC) != 0 {
			// zero width when Extended_Pictographic follows ZWJ
		} else if (cat & CAT_WIDTH_TWO) != 0 {
			width += 2;
		} else if (cat & CAT_WIDTH_ZERO) != 0 {
			// zero width
		} else {
			width += 1;
		}

		last_was_zwj = c == '\u{200D}';
		at += 1;
	}

	Ok(width)
}

fn is_hard_break(c: char) -> bool {
	matches!(c, '\n' | '\u{000B}' | '\u{000C}' | '\r' | '\u{0085}' | '\u{2028}' | '\u{2029}')
}

#[pyfunction(signature = (text, offset=0))]
fn line_next_hard_break(text: &str, offset: isize) -> PyResult<usize> {
	if offset < 0 {
		return Err(PyValueError::new_err("offset must be non-negative"));
	}
	let offset =
		usize::try_from(offset).map_err(|_| PyValueError::new_err("offset must be non-negative"))?;

	let chars = chars_of(text);
	if offset > chars.len() {
		return Err(PyValueError::new_err("offset is out of bounds"));
	}

	let mut i = offset;
	while i < chars.len() {
		if chars[i] == '\r' {
			if i + 1 < chars.len() && chars[i + 1] == '\n' {
				return Ok(i + 2);
			}
			return Ok(i + 1);
		}
		if is_hard_break(chars[i]) {
			return Ok(i + 1);
		}
		i += 1;
	}

	Ok(chars.len())
}

#[pyfunction(signature = (text, offset=0))]
fn line_next_break(py: Python<'_>, text: &str, offset: isize) -> PyResult<usize> {
	if offset < 0 {
		return Err(PyValueError::new_err("offset must be non-negative"));
	}
	let mut i =
		usize::try_from(offset).map_err(|_| PyValueError::new_err("offset must be non-negative"))?;
	let chars = chars_of(text);
	if i > chars.len() {
		return Err(PyValueError::new_err("offset is out of bounds"));
	}
	if i == chars.len() {
		return Ok(i);
	}

	if chars[i] == '\r' {
		if i + 1 < chars.len() && chars[i + 1] == '\n' {
			return Ok(i + 2);
		}
		return Ok(i + 1);
	}
	if is_hard_break(chars[i]) {
		return Ok(i + 1);
	}

	let start_class = break_class(py, chars[i])?;
	if start_class == BreakClass::Emoji {
		i += 1;
		while i < chars.len() {
			let cls = break_class(py, chars[i])?;
			if cls == BreakClass::Extend || chars[i - 1] == '\u{200D}' || cls == BreakClass::Emoji {
				i += 1;
				continue;
			}
			break;
		}
		return Ok(i);
	}

	let mut j = word_next_break(py, text, i as isize)?;
	if start_class == BreakClass::Extend
		&& chars[i] != '\u{200D}'
		&& !is_variation_selector(chars[i])
		&& j < chars.len()
		&& break_class(py, chars[j])? == BreakClass::AlphaNum
	{
		j = word_next_break(py, text, j as isize)?;
	}
	if j < chars.len() && break_class(py, chars[j])? == BreakClass::Punct {
		j += 1;
	}
	while j < chars.len() && chars[j].is_whitespace() {
		j += 1;
	}
	Ok(j)
}

#[pyfunction(signature = (text, offset=0))]
fn word_next_break(py: Python<'_>, text: &str, offset: isize) -> PyResult<usize> {
	if offset < 0 {
		return Err(PyValueError::new_err("offset must be non-negative"));
	}
	let mut i =
		usize::try_from(offset).map_err(|_| PyValueError::new_err("offset must be non-negative"))?;
	let chars = chars_of(text);
	if i > chars.len() {
		return Err(PyValueError::new_err("offset is out of bounds"));
	}
	if i == chars.len() {
		return Ok(i);
	}

	let start_class = break_class(py, chars[i])?;

	if start_class == BreakClass::Space {
		i += 1;
		while i < chars.len() && break_class(py, chars[i])? == BreakClass::Space {
			i += 1;
		}
		return Ok(i);
	}

	if start_class == BreakClass::Punct || start_class == BreakClass::Other {
		return Ok(i + 1);
	}

	if start_class == BreakClass::RegionalIndicator {
		if i + 1 < chars.len() && break_class(py, chars[i + 1])? == BreakClass::RegionalIndicator {
			return Ok(i + 2);
		}
		return Ok(i + 1);
	}

	if start_class == BreakClass::Extend {
		i += 1;
		while i < chars.len() && break_class(py, chars[i])? == BreakClass::Extend {
			i += 1;
		}
		if i < chars.len() && break_class(py, chars[i])? == BreakClass::Emoji {
			i += 1;
			while i < chars.len() {
				let cls = break_class(py, chars[i])?;
				if cls == BreakClass::Extend || chars[i - 1] == '\u{200D}' || cls == BreakClass::Emoji {
					i += 1;
					continue;
				}
				break;
			}
		}
		return Ok(i);
	}

	if start_class == BreakClass::Emoji {
		i += 1;
		while i < chars.len() {
			let cls = break_class(py, chars[i])?;
			if cls == BreakClass::Extend || chars[i - 1] == '\u{200D}' || cls == BreakClass::Emoji {
				i += 1;
				continue;
			}
			break;
		}
		if offset == 0 {
			while i < chars.len() && break_class(py, chars[i])? == BreakClass::AlphaNum {
				i += 1;
			}
		}
		return Ok(i);
	}

	i += 1;
	while i < chars.len() {
		let cls = break_class(py, chars[i])?;
		if cls == BreakClass::AlphaNum || cls == BreakClass::Extend {
			i += 1;
			continue;
		}
		break;
	}
	Ok(i)
}

#[pyfunction(signature = (text, offset=0))]
fn sentence_next_break(text: &str, offset: isize) -> PyResult<usize> {
	if offset < 0 {
		return Err(PyValueError::new_err("offset must be non-negative"));
	}
	let mut i =
		usize::try_from(offset).map_err(|_| PyValueError::new_err("offset must be non-negative"))?;
	let chars = chars_of(text);
	if i > chars.len() {
		return Err(PyValueError::new_err("offset is out of bounds"));
	}

	while i < chars.len() {
		i += 1;
		if i == chars.len() {
			return Ok(i);
		}
		let c = chars[i - 1];
		if is_hard_break(c) || matches!(c, '!' | '?' | '。' | '！' | '？') {
			while i < chars.len() && chars[i].is_whitespace() {
				i += 1;
			}
			return Ok(i);
		}
	}
	Ok(chars.len())
}

#[pyfunction]
fn version_added(_codepoint: &Bound<'_, PyAny>) -> Option<String> {
	None
}

#[pyfunction]
fn codepoint_name(py: Python<'_>, codepoint: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
	let c = extract_codepoint(codepoint)?;
	let result = unicode_data(py)?.call_method1("name", (c.to_string(), py.None()))?;
	if result.is_none() { Ok(None) } else { Ok(Some(result.extract()?)) }
}

#[pymodule(gil_used = false)]
fn _unicode(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
	module.add_class::<ToUtf8PositionMapper>()?;
	module.add_class::<FromUtf8PositionMapper>()?;
	module.add_class::<OffsetMapper>()?;

	module.add("unicode_version", UNICODE_VERSION)?;
	module.add("hard_breaks", PyFrozenSet::new(py, HARD_BREAKS)?)?;

	module.add_function(wrap_pyfunction!(category_name, module)?)?;
	module.add_function(wrap_pyfunction!(category_category, module)?)?;
	module.add_function(wrap_pyfunction!(sentence_next_break, module)?)?;
	module.add_function(wrap_pyfunction!(grapheme_next_break, module)?)?;
	module.add_function(wrap_pyfunction!(word_next_break, module)?)?;
	module.add_function(wrap_pyfunction!(line_next_break, module)?)?;
	module.add_function(wrap_pyfunction!(line_next_hard_break, module)?)?;
	module.add_function(wrap_pyfunction!(has_category, module)?)?;
	module.add_function(wrap_pyfunction!(casefold, module)?)?;
	module.add_function(wrap_pyfunction!(strip, module)?)?;
	module.add_function(wrap_pyfunction!(grapheme_length, module)?)?;
	module.add_function(wrap_pyfunction!(grapheme_substr, module)?)?;
	module.add_function(wrap_pyfunction!(text_width, module)?)?;
	module.add_function(wrap_pyfunction!(grapheme_find, module)?)?;
	module.add_function(wrap_pyfunction!(version_added, module)?)?;
	module.add_function(wrap_pyfunction!(codepoint_name, module)?)?;
	Ok(())
}
