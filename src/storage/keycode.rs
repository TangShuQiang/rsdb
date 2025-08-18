use serde::{
    de::{self, IntoDeserializer},
    ser,
};

use crate::error::{RSDBError, RSDBResult};

pub fn serialize_key<T: serde::Serialize>(key: &T) -> RSDBResult<Vec<u8>> {
    let mut ser = Serializer { output: Vec::new() };
    key.serialize(&mut ser)?;
    Ok(ser.output)
}

pub fn deserialize_key<'a, T: serde::Deserialize<'a>>(input: &'a [u8]) -> RSDBResult<T> {
    let mut der = Deserializer { input };
    T::deserialize(&mut der)
}

pub struct Serializer {
    output: Vec<u8>,
}

impl<'a> ser::Serializer for &'a mut Serializer {
    type Ok = ();

    type Error = RSDBError;

    type SerializeSeq = Self;

    type SerializeTuple = Self;

    type SerializeTupleVariant = Self;

    type SerializeTupleStruct = serde::ser::Impossible<Self::Ok, Self::Error>;

    type SerializeMap = serde::ser::Impossible<Self::Ok, Self::Error>;

    type SerializeStruct = serde::ser::Impossible<Self::Ok, Self::Error>;

    type SerializeStructVariant = serde::ser::Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, v: bool) -> RSDBResult<()> {
        self.output.push(v as u8);
        Ok(())
    }

    fn serialize_i8(self, _v: i8) -> RSDBResult<()> {
        todo!()
    }

    fn serialize_i16(self, _v: i16) -> RSDBResult<()> {
        todo!()
    }

    fn serialize_i32(self, _v: i32) -> RSDBResult<()> {
        todo!()
    }

    fn serialize_i64(self, v: i64) -> RSDBResult<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_u8(self, _v: u8) -> RSDBResult<()> {
        todo!()
    }

    fn serialize_u16(self, _v: u16) -> RSDBResult<()> {
        todo!()
    }

    fn serialize_u32(self, _v: u32) -> RSDBResult<()> {
        todo!()
    }

    fn serialize_u64(self, v: u64) -> RSDBResult<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, _v: f32) -> RSDBResult<()> {
        todo!()
    }

    fn serialize_f64(self, _v: f64) -> RSDBResult<()> {
        todo!()
    }

    fn serialize_char(self, _v: char) -> RSDBResult<()> {
        todo!()
    }

    fn serialize_str(self, v: &str) -> RSDBResult<()> {
        self.output.extend(v.as_bytes());
        Ok(())
    }

    // 原始值           编码后
    // 97 98 99     -> 97 98 99 0 0
    // 97 98 0 99   -> 97 98 0 255 99 0 0
    // 97 98 0 0 99 -> 97 98 0 255 0 255 99 0 0
    fn serialize_bytes(self, v: &[u8]) -> RSDBResult<()> {
        let mut res = Vec::new();
        for e in v.into_iter() {
            match e {
                0 => res.extend([0, 255]),
                b => res.push(*b),
            }
        }
        // 放 0 0 表示结尾
        res.extend([0, 0]);

        self.output.extend(res);
        Ok(())
    }

    fn serialize_none(self) -> RSDBResult<()> {
        todo!()
    }

    fn serialize_some<T>(self, _value: &T) -> RSDBResult<()>
    where
        T: ?Sized + ser::Serialize,
    {
        todo!()
    }

    fn serialize_unit(self) -> RSDBResult<()> {
        todo!()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> RSDBResult<()> {
        todo!()
    }

    // 类似 MvccKey::NextVersion
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> RSDBResult<()> {
        self.output.extend(u8::try_from(variant_index));
        Ok(())
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, _value: &T) -> RSDBResult<()>
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
    ) -> RSDBResult<()>
    where
        T: ?Sized + ser::Serialize,
    {
        self.serialize_unit_variant(name, variant_index, variant)?;
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> RSDBResult<Self::SerializeSeq> {
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> RSDBResult<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> RSDBResult<Self::SerializeTupleStruct> {
        todo!()
    }

    // 类似 TxnWrite(Version, Vec<u8>)
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> RSDBResult<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> RSDBResult<Self::SerializeMap> {
        todo!()
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> RSDBResult<Self::SerializeStruct> {
        todo!()
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> RSDBResult<Self::SerializeStructVariant> {
        todo!()
    }
}

impl<'a> ser::SerializeSeq for &'a mut Serializer {
    type Ok = ();

    type Error = RSDBError;

    fn serialize_element<T>(&mut self, value: &T) -> RSDBResult<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> RSDBResult<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut Serializer {
    type Ok = ();

    type Error = RSDBError;

    fn serialize_element<T>(&mut self, value: &T) -> RSDBResult<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> RSDBResult<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut Serializer {
    type Ok = ();

    type Error = RSDBError;

    fn serialize_field<T>(&mut self, value: &T) -> RSDBResult<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> RSDBResult<()> {
        Ok(())
    }
}

pub struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    fn take_bytes(&mut self, len: usize) -> &[u8] {
        let bytes = &self.input[..len];
        self.input = &self.input[len..];
        bytes
    }

    // - 如果这个 0 之后的值是 255，说明是原始字符串中的 0，则继续解析
    // - 如果这个 0 之后的值是 0，说明是字符串的结尾
    fn next_bytes(&mut self) -> RSDBResult<Vec<u8>> {
        let mut res = Vec::new();
        let mut iter = self.input.iter().enumerate();
        let i = loop {
            match iter.next() {
                Some((_, 0)) => match iter.next() {
                    Some((i, 0)) => break i + 1,
                    Some((_, 255)) => res.push(0),
                    _ => return Err(RSDBError::Internal("unexpected input".into())),
                },
                Some((_, b)) => res.push(*b),
                _ => return Err(RSDBError::Internal("unexpected input".into())),
            }
        };
        self.input = &self.input[i..];
        Ok(res)
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = RSDBError;

    fn deserialize_any<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let v = self.take_bytes(1)[0];
        // v == 0 => false
        // 否则为 true
        visitor.visit_bool(v != 0)
    }

    fn deserialize_i8<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i16<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i32<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.take_bytes(8);
        let v = i64::from_be_bytes(bytes.try_into()?);
        visitor.visit_i64(v)
    }

    fn deserialize_u8<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u16<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u32<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    // &[u8] -> Vec<u8>
    // From TryFrom
    fn deserialize_u64<V>(self, visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.take_bytes(8);
        let v = u64::from_be_bytes(bytes.try_into()?);
        visitor.visit_u64(v)
    }

    fn deserialize_f32<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f64<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_char<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        let bytes = self.next_bytes()?;
        visitor.visit_str(&String::from_utf8(bytes)?)
    }

    fn deserialize_string<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bytes<V>(self, visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_bytes(&self.next_bytes()?)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_byte_buf(self.next_bytes()?)
    }

    fn deserialize_option<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> RSDBResult<V::Value>
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
    ) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, _visitor: V) -> RSDBResult<V::Value>
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
    ) -> RSDBResult<V::Value>
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
    ) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }
}

impl<'de, 'a> de::SeqAccess<'de> for Deserializer<'de> {
    type Error = RSDBError;

    fn next_element_seed<T>(&mut self, seed: T) -> RSDBResult<Option<T::Value>>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(self).map(Some)
    }
}

impl<'de, 'a> de::EnumAccess<'de> for &mut Deserializer<'de> {
    type Error = RSDBError;

    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> RSDBResult<(V::Value, Self::Variant)>
    where
        V: de::DeserializeSeed<'de>,
    {
        let index = self.take_bytes(1)[0] as u32;
        let varint_index: RSDBResult<_> = seed.deserialize(index.into_deserializer());
        Ok((varint_index?, self))
    }
}

impl<'de, 'a> de::VariantAccess<'de> for &mut Deserializer<'de> {
    type Error = RSDBError;

    fn unit_variant(self) -> RSDBResult<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> RSDBResult<T::Value>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> RSDBResult<V::Value>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{
        keycode::{deserialize_key, serialize_key},
        mvcc::{MvccKey, MvccKeyPrefix},
    };

    #[test]
    fn test_encode() {
        let ser_cmp = |k: MvccKey, v: Vec<u8>| {
            let res = serialize_key(&k).unwrap();
            assert_eq!(res, v);
        };

        ser_cmp(MvccKey::NextVersion, vec![0]);
        ser_cmp(MvccKey::TxnActive(1), vec![1, 0, 0, 0, 0, 0, 0, 0, 1]);
        ser_cmp(
            MvccKey::TxnWrite(1, vec![1, 2, 3]),
            vec![2, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 0, 0],
        );
        ser_cmp(
            MvccKey::Version(b"abc".to_vec(), 11),
            vec![3, 97, 98, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11],
        );
    }

    #[test]
    fn test_encode_prefix() {
        let ser_cmp = |k: MvccKeyPrefix, v: Vec<u8>| {
            let res = serialize_key(&k).unwrap();
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

    #[test]
    fn test_decode() {
        let der_cmp = |k: MvccKey, v: Vec<u8>| {
            let res: MvccKey = deserialize_key(&v).unwrap();
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

    // #[test]
    // fn test_u8_convert() {
    //     let v = [1 as u8, 2, 3];
    //     let vv = &v;
    //     let vvv: Vec<u8> = vv.try_into().unwrap();
    //     println!("{:?}", vvv);
    // }
}
