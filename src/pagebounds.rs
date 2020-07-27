use std::cmp::Ordering;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub enum Marker {
    NegInf,
    Finite(String),
    PosInf,
}

impl Marker {
    pub fn cmp_str(&self, other: &str) -> Ordering {
        match self {
            Marker::NegInf => Ordering::Less,
            Marker::Finite(s) => s.as_str().cmp(other),
            Marker::PosInf => Ordering::Greater,
        }
    }

    pub fn serialize(&self, encoded: &mut Vec<u8>) {
        match self {
            Marker::PosInf => encoded.push(0),
            Marker::Finite(url) => {
                encoded.push(1);
                encoded.push(url.len() as u8);
                encoded.extend_from_slice(url.as_bytes());
            },
            Marker::NegInf => encoded.push(2),
        }
    }

    pub fn deserialize(i: &mut usize, encoded: &[u8]) -> Marker {
        match encoded[*i] {
            0 => {
                *i += 1;
                Marker::PosInf
            },
            1 => {
                *i += 1;
                let url_len = encoded[*i] as usize;
                *i += 1;
                let res = Marker::Finite(String::from_utf8(encoded[*i..(*i + url_len)].to_vec()).unwrap());
                *i += url_len;
                res
            },
            2 => {
                *i += 1;
                Marker::NegInf
            },
            _ => panic!("failed to deserialize marker"),
        }
    }
}

#[derive(Eq, Clone, Debug)]
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

    fn cmp_value(&self, other: &str) -> Ordering {
        // left <= other < right
        if self.left.cmp_str(other) != Ordering::Greater && self.right.cmp_str(other) == Ordering::Greater {
            return Ordering::Equal;
        }
        // other < self.left
        if self.left.cmp_str(other) == Ordering::Greater {
            return Ordering::Less;
        }
        // other >= right
        return Ordering::Greater;
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        self.left.serialize(&mut encoded);
        self.right.serialize(&mut encoded);
        encoded
    }

    pub fn deserialize(encoded: &[u8]) -> PageBounds {
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
        // println!("failing at comparison of {:?} with {:?}", self, other);
        panic!("cannot compare partially-overlapping pages");
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

#[derive(Eq, Clone, Debug)]
pub enum PageBoundsCmp {
    Bounds(PageBounds),
    Value(String),
}

impl PageBoundsCmp {
    pub fn init() -> PageBoundsCmp {
        PageBoundsCmp::Bounds(PageBounds::new(Marker::NegInf, Marker::PosInf))
    }
}

impl Ord for PageBoundsCmp {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (PageBoundsCmp::Bounds(a), PageBoundsCmp::Bounds(b)) => a.cmp(b),
            (PageBoundsCmp::Bounds(a), PageBoundsCmp::Value(b)) => a.cmp_value(b),
            (PageBoundsCmp::Value(a), PageBoundsCmp::Bounds(b)) => b.cmp_value(a).reverse(),
            (PageBoundsCmp::Value(a), PageBoundsCmp::Value(b)) => a.cmp(b),
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

