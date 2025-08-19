use serde::ser;

use crate::error::Error;
use crate::error::Result;

pub fn serialize<T: serde::Serialize>(key: T) -> Result<Vec<u8>> {
    let mut ser = Serializer { output: Vec::new() };
    key.serialize(&mut ser)?;
    Ok(ser.output)
}

pub struct Serializer {
    output: Vec<u8>,
}

// 实现序列化, 内部使用自定义的 Serializer, 把对象编码到 Vec<u8>。
// 调用方传入任何实现了 serde::Serialize 的对象（比如 MvccKey 枚举）。
// 当 serde 的派生宏（或手动实现的 Serialize）序列化某个字段时，框架会调用对应的 serialize_xxx 方法。
// serialize_u64：当序列化一个 u64 时被调用 → 写成大端字节序到 output。
// serialize_bytes：处理字节数组，带有特殊的转义规则。
// serialize_unit_variant：枚举的「无参数变体」（比如 MvccKey::NextVersion） → 写入变体索引。
// serialize_newtype_variant：枚举的「单字段变体」（比如 TxnActive(u64)） → 先写索引，再序列化字段。
// serialize_tuple_variant：枚举的「元组变体」（比如 TxnWrite(u64, Vec<u8>)） → 写索引，再依次写各字段。
impl<'a> ser::Serializer for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleVariant = Self;
    type SerializeTupleStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = serde::ser::Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, _v: bool) -> Result<()> {
        todo!()
    }

    fn serialize_i8(self, _v: i8) -> Result<()> {
        todo!()
    }

    fn serialize_i16(self, _v: i16) -> Result<()> {
        todo!()
    }

    fn serialize_i32(self, _v: i32) -> Result<()> {
        todo!()
    }

    fn serialize_i64(self, _v: i64) -> Result<()> {
        todo!()
    }

    fn serialize_u8(self, _v: u8) -> Result<()> {
        todo!()
    }

    fn serialize_u16(self, _v: u16) -> Result<()> {
        todo!()
    }

    fn serialize_u32(self, _v: u32) -> Result<()> {
        todo!()
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, _v: f32) -> Result<()> {
        todo!()
    }

    fn serialize_f64(self, _v: f64) -> Result<()> {
        todo!()
    }

    fn serialize_char(self, _v: char) -> Result<()> {
        todo!()
    }

    fn serialize_str(self, _v: &str) -> Result<()> {
        todo!()
    }

    // 00 表示结束，原值[0]转换为[0,255]
    // 原始值           编码后
    // 97 98 99     -> 97 98 99 0 0
    // 97 98 0 99   -> 97 98 0 255 99 0 0
    // 97 98 0 0 99 -> 97 98 0 255 0 255 99 0 0
    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        let mut res = Vec::new();
        for e in v.into_iter() {
            match e {
                0 => res.extend([0, 255]),
                b => res.push(*b),
            }
        }
        // 放 [0, 0] 表示结尾
        res.extend([0, 0]);

        self.output.extend(res);
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        todo!()
    }

    fn serialize_some<T>(self, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        todo!()
    }

    fn serialize_unit(self) -> Result<()> {
        todo!()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        todo!()
    }

    // 类似 MvccKey::NextVersion
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        self.output.extend(u8::try_from(variant_index));
        Ok(())
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        todo!()
    }

    // 类似 TxnAcvtive(Version)
    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        // 处理 index
        self.serialize_unit_variant(name, variant_index, variant)?;
        // 处理值
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        todo!()
    }

    // 类似 TxnWrite(Version, Vec<u8>)
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        Ok(self) // 把自身作为 tuple serializer 返回
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        todo!()
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        todo!()
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        todo!()
    }
}

// 表示「序列 / 切片」类型，比如 Vec<T>、&[T]、HashSet<T>
impl<'a> ser::SerializeSeq for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

// 表示「固定长度元组」，比如 (u32, String)、(i64, i64, i64)。
impl<'a> ser::SerializeTuple for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

// 表示「枚举里的元组变体」，比如：enum MyEnum {A(u64, Vec<u8>),}
impl<'a> ser::SerializeTupleVariant for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::serialize;

    use crate::storage::mvcc::{MvccKey, MvccKeyPrefix};

    #[test]
    fn test_encode() {
        let ser_cmp = |k: MvccKey, v: Vec<u8>| {
            let res = serialize(&k).unwrap();
            println!("{:?}", res);
            assert_eq!(res, v);
        };

        // serialize(&NextVersion)
        //  → Serializer::serialize_unit_variant("MvccKey", idx=0, "NextVersion")
        //      └ 写出 variant_index (0)
        ser_cmp(MvccKey::NextVersion, vec![0]);

        // serialize(&TxnActive(1))
        //   → Serializer::serialize_newtype_variant("MvccKey", idx=1, "TxnActive", &1)
        //       ├ Serializer::serialize_unit_variant("MvccKey", idx=1, "TxnActive")
        //       │   └ 写出 variant_index (1)
        //       └ value.serialize(self)  // 这里 value = 1 (u64)
        //           → Serializer::serialize_u64(1)
        //               └ 写出 [0,0,0,0,0,0,0,1]
        ser_cmp(MvccKey::TxnActive(1), vec![1, 0, 0, 0, 0, 0, 0, 0, 1]);

        // serialize(&TxnWrite(1, vec![1,2,3]))
        //  → Serializer::serialize_tuple_variant("MvccKey", idx=2, "TxnWrite", len=2)
        //      ├ Serializer::serialize_unit_variant("MvccKey", idx=2, "TxnWrite")
        //      │   └ 写出 variant_index (2)
        //      └ 返回一个 &mut Serializer (impl SerializeTupleVariant)
        //         接下来逐个字段：
        //         - serialize_field(&1)
        //             → Serializer::serialize_u64(1)
        //                 └ 写出 [0,0,0,0,0,0,0,1]
        //         - serialize_field(&vec![1,2,3])
        //             → Serializer::serialize_bytes(&[1,2,3])
        //                 └ 写出 [1,2,3, 0,0]   // 注意 0,0 作为结尾标志
        //         - end()
        ser_cmp(
            MvccKey::TxnWrite(1, vec![1, 2, 3]),
            vec![2, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 0, 0],
        );

        // serialize(&Version(b"abc".to_vec(), 11))
        //  → Serializer::serialize_tuple_variant("MvccKey", idx=3, "Version", len=2)
        //      ├ serialize_unit_variant("MvccKey", idx=3, "Version")
        //      │   └ 写出 variant_index (3)
        //      └ 返回 SerializeTupleVariant
        //         - serialize_field(&b"abc".to_vec())
        //             → Serializer::serialize_bytes(&[97,98,99])
        //                 └ 写出 [97,98,99, 0,0]   // 按照 0 → [0,255], 结尾补 [0,0]
        //         - serialize_field(&11)
        //             → Serializer::serialize_u64(11)
        //                 └ 写出 [0,0,0,0,0,0,0,11]
        //         - end()
        ser_cmp(
            MvccKey::Version(b"abc".to_vec(), 11),
            vec![3, 97, 98, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11],
        );
    }

    #[test]
    fn test_encode_prefix() {
        let ser_cmp = |k: MvccKeyPrefix, v: Vec<u8>| {
            let res = serialize(&k).unwrap();
            println!("{:?}", res);
            assert_eq!(res, v);
        };

        ser_cmp(MvccKeyPrefix::NextVersion, vec![0]);
        ser_cmp(MvccKeyPrefix::TxnActive, vec![1]);
        ser_cmp(MvccKeyPrefix::TxnWrite(1), vec![2, 0, 0, 0, 0, 0, 0, 0, 1]);
        ser_cmp(
            MvccKeyPrefix::Version(b"ab".to_vec()),
            vec![3, 97, 98, 0, 0],
        );
    }
}
