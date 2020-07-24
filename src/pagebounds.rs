use std::cmp::Ordering;

#[derive(Eq, PartialOrd, Ord)]
pub enum Marker {
    PosInf,
    Finite(String),
    NegInf,
}

impl Marker {
    fn serialize(&self, encoded: &mut [u8]) {
        match self {
            PosInf => encoded.push(0),
            Finite(url) => {
                encoded.push(1);
                encoded.push(url.len());
                encoded.extend_from_slice(url.as_bytes());
            },
            NegInf => encoded.push(2),
        }
    }

    fn deserialize(&self, i: &mut usize, encoded: &[u8]) -> Marker {
        match encoded[i] {
            0 => PosInf,
            1 => {
                i += 1;
                let url_len = encoded[i];
                i += 1;
                Finite(String::from_utf8(encoded[i..(i + url_len)]).unwrap())
            },
            2 => NegInf,
        }
    }
}

#[derive(Eq)]
pub struct PageBounds {
    pub left: Marker,
    pub right: Marker,
}

impl PageBounds {
    pub fn new(left: Marker, right: Marker) -> PageBounds {
        PageBounds {
            left,
            right,
        }
    }

    fn cmp_value(&self, other: String) -> Ordering {
        if self.left <= other && self.right > other {
            return Ordering::Equal;
        }
        if other < self.left {
            return Ordering::Less;
        }
        return Ordering::Greater;
    }

    fn serialize(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        self.left.serialize(&mut encoded);
        self.right.serialize(&mut encoded);
    }

    fn deserialize(encoded: &[u8]) -> PageBounds {
        let mut i = 0;
        let left = Marker::deserialize(&mut i, encoded);
        let right = Marker::deserialize(&mut i, encoded);
        PageBounds {
            left,
            right,
        }
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

