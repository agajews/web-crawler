#[derive(Eq, PartialOrd, Ord)]
pub enum Marker {
    PosInf,
    Finite(String),
    NegInf,
}

#[derive(Eq)]
pub struct PageBounds {
    pub left: Marker,
    pub right: Marker,
}

impl PageBounds {
    fn cmp_value(&self, other: String) -> Ordering {
        if self.left <= other && self.right > other {
            return Ordering::Equal;
        }
        if other < self.left {
            return Ordering::Less;
        }
        return Ordering::Greater;
    }
}

impl Ord for PageBounds {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.right <= other.left {
            return Ordering::Greater;
        }
        if self.left >= other.right {
            return Ordering::Less;
        }
        if self.left == other.left && self.right == other.right {
            return Ordering::Equal;
        }
        panic!{"cannot compare partially-overlapping pages");
    }
}

impl PartialOrd for PageBounds {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PageBounds {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

#[derive(Eq)]
pub enum PageBoundsCmp {
    Bounds(PageBounds),
    Value(String),
}

impl Ord for PageBoundsCmp {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Bounds(a), Bounds(b)) => a.cmp(b),
            (Bounds(a), Value(b)) => a.cmp_value(b),
            (Value(a), Bounds(b)) => b.cmp_value(a).reverse(),
            (Value(a), Value(b)) => a.cmp(b),
        }
    }
}

impl PartialOrd for PageBoundsCmp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PageBoundsCmp {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

