use serde::de;
use serde::de::IntoDeserializer;

use crate::error::Error;
use crate::error::Result;

pub struct Deserializer<'de> {
    input: &'de [u8],
}

// tip
pub fn deserialize<'a, T: serde::Deserialize<'a>>(input: &'a [u8]) -> Result<T> {
    let mut der = Deserializer { input };
    T::deserialize(&mut der)
}

impl<'de> Deserializer<'de> {
    fn take_bytes(&mut self, len: usize) -> &[u8] {
        let bytes = &self.input[..len];
        self.input = &self.input[len..];
        bytes
    }

    // - 如果这个 0 之后是 255，说明是原始字符串中的 0， 则继续解析
    // - 如果这个 0 之后是 0，说明是字符串的结尾
    fn next_bytes(&mut self) -> Result<Vec<u8>> {
        let mut res = Vec::new();
        let mut iter = self.input.iter().enumerate();

        let i = loop {
            match iter.next() {
                Some((_, 0)) => match iter.next() {
                    Some((i, 0)) => break i + 1,
                    Some((_, 255)) => res.push(0),
                    _ => return Err(Error::Internal("Unexpected byte".into())),
                },
                Some((_, b)) => res.push(*b),
                _ => return Err(Error::Internal("Unexpected byte".into())),
            }
        };

        self.input = &self.input[i..];
        Ok(res)
    }
}

///
/// 生命周期说明
/// - 'de 管输入数据能活多久
/// - 'a 管“反序列化器”这次可变借用能活多久
impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bool<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i16<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i64<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    // &[u8] -> Vec<u8>
    // From TryFrom
    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.take_bytes(8);
        let v = u64::from_be_bytes(bytes.try_into()?);
        visitor.visit_u64(v)
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_string<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_bytes(&self.next_bytes()?)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_byte_buf(self.next_bytes()?)
    }

    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }
}

impl<'de, 'a> de::SeqAccess<'de> for Deserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(
        &mut self,
        send: T,
    ) -> std::result::Result<Option<T::Value>, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        send.deserialize(self).map(Some)
    }
}

impl<'de, 'a> de::EnumAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;

    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: de::DeserializeSeed<'de>,
    {
        let index = self.take_bytes(1)[0] as u32;
        let varint_index: Result<_> = seed.deserialize(index.into_deserializer());
        Ok((varint_index?, self))
    }
}

impl<'de, 'a> de::VariantAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> std::result::Result<T::Value, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use crate::storage::{keycode_de::deserialize, mvcc::MvccKey};

    #[test]
    fn test_u8_convert() {
        let v = [1 as u8, 2, 3];
        let vv = &v;
        let vvv: Vec<u8> = vv.try_into().unwrap();
        println!("{:?}", vvv);
    }

    #[test]
    fn test_decode() {
        let der_cmp = |k: MvccKey, v: Vec<u8>| {
            let res: MvccKey = deserialize(&v).unwrap();
            assert_eq!(res, k);
        };

        der_cmp(MvccKey::NextVersion, vec![0]);
        der_cmp(MvccKey::TxnActive(1), vec![1, 0, 0, 0, 0, 0, 0, 0, 1]);
        der_cmp(
            MvccKey::TxnWrite(1, vec![1, 2, 3]),
            vec![2, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 0, 0],
        );
        der_cmp(
            MvccKey::Version(b"abc".to_vec(), 11),
            vec![3, 97, 98, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11],
        );
    }
}
