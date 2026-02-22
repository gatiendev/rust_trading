use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct Kline {
    pub open_time: i64, // milliseconds
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub close_time: i64, // milliseconds
}

impl<'de> Deserialize<'de> for Kline {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct KlineVisitor;

        impl<'de> serde::de::Visitor<'de> for KlineVisitor {
            type Value = Kline;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an array of 12 values")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Kline, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let open_time: i64 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let open: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let high: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                let low: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(3, &self))?;
                let close: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(4, &self))?;
                let volume: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(5, &self))?;
                let close_time: i64 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(6, &self))?;
                // Skip remaining 5 fields (quote volume, trades, etc.)
                for _ in 0..5 {
                    let _: serde_json::Value = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(7, &self))?;
                }

                Ok(Kline {
                    open_time,
                    open: open.parse().map_err(serde::de::Error::custom)?,
                    high: high.parse().map_err(serde::de::Error::custom)?,
                    low: low.parse().map_err(serde::de::Error::custom)?,
                    close: close.parse().map_err(serde::de::Error::custom)?,
                    volume: volume.parse().map_err(serde::de::Error::custom)?,
                    close_time,
                })
            }
        }

        deserializer.deserialize_seq(KlineVisitor)
    }
}
