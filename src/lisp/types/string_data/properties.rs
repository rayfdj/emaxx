//! The string header's single property pointer. Empty strings and strings
//! without intervals allocate no property container; intervals whose
//! plists are all empty keep one of no spans. The existing span
//! representation remains authoritative until it is replaced by intervals.

use super::StringPropertySpan;
use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::ptr::NonNull;

/// One allocation contains the span count followed by its actual span values.
/// No Vec descriptor, spare capacity or second span allocation is retained.
#[repr(C)]
struct PropertyData {
    len: usize,
}

fn property_layout(len: usize) -> (Layout, usize) {
    let (layout, offset) = Layout::new::<PropertyData>()
        .extend(Layout::array::<StringPropertySpan>(len).expect("string property span layout"))
        .expect("string property allocation layout");
    (layout.pad_to_align(), offset)
}

#[repr(transparent)]
#[derive(Default)]
pub struct StringProperties(Option<NonNull<PropertyData>>);

impl Clone for StringProperties {
    fn clone(&self) -> Self {
        if self.has_intervals() && self.is_empty() {
            return Self::empty_tree();
        }
        self.to_vec().into()
    }
}

impl StringProperties {
    /// Intervals every plist of which is empty: what textprop.c leaves of
    /// a string's intervals when their properties are removed, until
    /// `set-text-properties' clears the whole string.
    pub fn empty_tree() -> Self {
        let (layout, _) = property_layout(0);
        // SAFETY: the layout is nonzero; it holds the count alone.
        let allocation = unsafe { alloc(layout) };
        let Some(data) = NonNull::new(allocation.cast::<PropertyData>()) else {
            handle_alloc_error(layout);
        };
        // SAFETY: the allocation is aligned for its count.
        unsafe { data.as_ptr().write(PropertyData { len: 0 }) };
        Self(Some(data))
    }

    /// Whether the string has intervals (string_intervals non-null),
    /// though every plist be empty.
    pub fn has_intervals(&self) -> bool {
        self.0.is_some()
    }
}

impl std::fmt::Debug for StringProperties {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(formatter)
    }
}

impl PartialEq for StringProperties {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl Drop for StringProperties {
    fn drop(&mut self) {
        if let Some(data) = self.0 {
            // SAFETY: this unique owner initialized LEN spans in this exact
            // layout. Dropping them releases their owned property lists.
            unsafe {
                let len = data.as_ref().len;
                let (layout, offset) = property_layout(len);
                let spans = data
                    .as_ptr()
                    .cast::<u8>()
                    .add(offset)
                    .cast::<StringPropertySpan>();
                std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(spans, len));
                dealloc(data.as_ptr().cast(), layout);
            }
        }
    }
}

impl From<Vec<StringPropertySpan>> for StringProperties {
    fn from(mut spans: Vec<StringPropertySpan>) -> Self {
        let len = spans.len();
        if len == 0 {
            return Self::default();
        }
        let (layout, offset) = property_layout(len);
        // SAFETY: the layout is nonzero and includes its count and all spans.
        let allocation = unsafe { alloc(layout) };
        let Some(data) = NonNull::new(allocation.cast::<PropertyData>()) else {
            handle_alloc_error(layout);
        };
        // SAFETY: move all initialized spans into disjoint, aligned storage.
        // Clearing the source length leaves its buffer freeable without
        // dropping values now owned by PropertyData.
        unsafe {
            data.as_ptr().write(PropertyData { len });
            std::ptr::copy_nonoverlapping(spans.as_ptr(), allocation.add(offset).cast(), len);
            spans.set_len(0);
        }
        Self(Some(data))
    }
}

use std::ops::Deref;

impl Deref for StringProperties {
    type Target = [StringPropertySpan];

    fn deref(&self) -> &Self::Target {
        let Some(data) = self.0 else {
            return &[];
        };
        // SAFETY: the unique owner has LEN initialized spans, and a shared
        // property borrow excludes all mutations and destruction.
        unsafe {
            let len = data.as_ref().len;
            let (_, offset) = property_layout(len);
            std::slice::from_raw_parts(data.as_ptr().cast::<u8>().add(offset).cast(), len)
        }
    }
}

impl std::ops::DerefMut for StringProperties {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let Some(data) = self.0 else {
            return &mut [];
        };
        // SAFETY: this mutable borrow exclusively owns all initialized spans.
        unsafe {
            let len = data.as_ref().len;
            let (_, offset) = property_layout(len);
            std::slice::from_raw_parts_mut(data.as_ptr().cast::<u8>().add(offset).cast(), len)
        }
    }
}

impl<'a> IntoIterator for &'a StringProperties {
    type Item = &'a StringPropertySpan;
    type IntoIter = std::slice::Iter<'a, StringPropertySpan>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoIterator for &'a mut StringProperties {
    type Item = &'a mut StringPropertySpan;
    type IntoIter = std::slice::IterMut<'a, StringPropertySpan>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

const _: () = assert!(std::mem::size_of::<StringProperties>() == std::mem::size_of::<usize>());
