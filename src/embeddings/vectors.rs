//! Vector representation (spec 12.2): float32 little-endian BLOBs, unit length, dot-product ranking.

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum VectorError {
    #[error("vector is empty")]
    Empty,
    #[error("vector contains NaN or infinite values")]
    NotFinite,
    #[error("vector has zero norm")]
    ZeroNorm,
}

pub fn encode_le_f32(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// `None` when the byte length is not a multiple of four.
pub fn decode_le_f32(bytes: &[u8]) -> Option<Vec<f32>> {
    if bytes.len() % 4 != 0 {
        return None;
    }
    Some(
        bytes
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect(),
    )
}

/// The single normalization implementation: rejects empty/non-finite/zero-norm vectors, then scales to unit
/// length. The norm is accumulated in f64 so large-magnitude inputs do not overflow.
pub fn normalize(v: &mut [f32]) -> Result<(), VectorError> {
    if v.is_empty() {
        return Err(VectorError::Empty);
    }
    if v.iter().any(|x| !x.is_finite()) {
        return Err(VectorError::NotFinite);
    }
    let norm = v
        .iter()
        .map(|&x| (x as f64) * (x as f64))
        .sum::<f64>()
        .sqrt();
    if !norm.is_finite() || norm == 0.0 {
        return Err(if norm == 0.0 {
            VectorError::ZeroNorm
        } else {
            VectorError::NotFinite
        });
    }
    for x in v.iter_mut() {
        *x = ((*x as f64) / norm) as f32;
    }
    Ok(())
}

pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_normalize() {
        let mut v = vec![3.0f32, 4.0];
        normalize(&mut v).unwrap();
        assert!((dot(&v, &v) - 1.0).abs() < 1e-6);
        assert_eq!(decode_le_f32(&encode_le_f32(&v)).unwrap(), v);
        assert!(decode_le_f32(&[1, 2, 3]).is_none());
        assert_eq!(normalize(&mut [0.0, 0.0]), Err(VectorError::ZeroNorm));
        assert_eq!(normalize(&mut [f32::NAN, 1.0]), Err(VectorError::NotFinite));
    }
}
