//! The string header's single property pointer: string_intervals, null
//! for a string without intervals, else its intervals over [0, SCHARS)
//! (see intervals.rs), every plist of which may be empty.

use crate::intervals::Intervals;

#[repr(transparent)]
#[derive(Default)]
pub struct StringProperties(Option<Box<Intervals>>);

impl StringProperties {
    /// The string's intervals INTERVALS, or none.
    pub(crate) fn new(intervals: Option<Intervals>) -> Self {
        Self(intervals.map(Box::new))
    }

    /// Whether the string has intervals (string_intervals non-null),
    /// though every plist be empty.
    pub fn has_intervals(&self) -> bool {
        self.0.is_some()
    }

    pub(crate) fn intervals(&self) -> Option<&Intervals> {
        self.0.as_deref()
    }

    pub(crate) fn intervals_mut(&mut self) -> Option<&mut Intervals> {
        self.0.as_deref_mut()
    }

    /// set_string_intervals.
    pub(crate) fn set(&mut self, intervals: Option<Intervals>) {
        self.0 = intervals.map(Box::new);
    }

    /// The intervals, taken out.
    pub(crate) fn take(&mut self) -> Option<Intervals> {
        self.0.take().map(|intervals| *intervals)
    }

    /// Whether some interval has a property.
    pub(crate) fn has_properties(&self) -> bool {
        self.intervals()
            .is_some_and(|intervals| intervals.plists().any(|plist| !plist.is_nil()))
    }

    /// How many intervals there are (for the census).
    pub(crate) fn interval_count(&self) -> usize {
        self.intervals().map_or(0, Intervals::count)
    }
}

impl std::fmt::Debug for StringProperties {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.intervals() {
            None => formatter.write_str("no intervals"),
            Some(intervals) => formatter
                .debug_list()
                .entries(
                    intervals
                        .iter()
                        .map(|(start, end, plist)| (start, end, plist.to_string())),
                )
                .finish(),
        }
    }
}

impl PartialEq for StringProperties {
    fn eq(&self, other: &Self) -> bool {
        match (self.intervals(), other.intervals()) {
            (None, None) => true,
            (Some(left), Some(right)) => {
                left.end() == right.end()
                    && left.count() == right.count()
                    && left
                        .iter()
                        .zip(right.iter())
                        .all(|(left, right)| left.0 == right.0 && left.2 == right.2)
            }
            _ => false,
        }
    }
}

const _: () = assert!(std::mem::size_of::<StringProperties>() == std::mem::size_of::<usize>());
