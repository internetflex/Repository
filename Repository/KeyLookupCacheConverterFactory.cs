using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Repository
{
    internal class KeyConverter : JsonConverter<Key>
    {
        public override Key Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.Number:
                {
                    var uInt32 = reader.GetUInt32();
                    return new Key(uInt32);
                }
                case JsonTokenType.PropertyName:
                { // Dictionary keys are in quotes
                    var uintStr = reader.GetString();
                    var key = uint.Parse(uintStr);
                    return new Key(key);
                }
                default:
                    throw new JsonException($"Cannot convert token type {reader.TokenType} to Key");
            }
        }

        public override void Write(Utf8JsonWriter writer, Key value, JsonSerializerOptions options)
        {
            writer.WriteNumberValue(value.Value);
        }
    }

    internal class KeyLookupCacheConverterFactory : JsonConverterFactory
    {
        public override bool CanConvert(Type type)
        {
            if (type == typeof(Key))
                return true;

            if (!type.IsGenericType)
                return false;

            if (type.GetGenericTypeDefinition() == typeof(Dictionary<,>) && type.GetGenericArguments()[0] == typeof(Key))
                return true;

            return type.GetGenericTypeDefinition() == typeof(Queue<>) && type.GetGenericArguments()[0] == typeof(Key);
        }

        public override JsonConverter CreateConverter(Type type, JsonSerializerOptions options)
        {
            if (type == typeof(Key))
            {
                return new KeyConverter();
            }

            if (type.GetGenericTypeDefinition() == typeof(Dictionary<,>) && type.GetGenericArguments()[0] == typeof(Key))
            {
                var keyType = type.GetGenericArguments()[0];
                var valueType = type.GetGenericArguments()[1];

                var converter = (JsonConverter)Activator.CreateInstance(typeof(DictionaryKeyConverter<,>)
                        .MakeGenericType(new Type[] { keyType, valueType }), BindingFlags.Instance | BindingFlags.Public,
                        binder: null, args: new object[] { options }, culture: null);
                return converter;
            }

            if (type.GetGenericTypeDefinition() == typeof(Queue<>) && type.GetGenericArguments()[0] == typeof(Key))
            {
                var keyType = type.GetGenericArguments()[0];

                var converter = (JsonConverter)Activator.CreateInstance(typeof(QueueKeyConverter<>)
                        .MakeGenericType(keyType), BindingFlags.Instance | BindingFlags.Public,
                        binder: null, args: new object[] { options }, culture: null);
                return converter;
            }

            return null;
        }

        private class DictionaryKeyConverter<TKey, TValue> : JsonConverter<Dictionary<TKey, TValue>> where TKey : struct
        {
            private readonly JsonConverter<TValue> _valueConverter;
            private readonly JsonConverter<TKey> _keyConverter;
            private readonly Type _keyType;
            private readonly Type _valueType;

            public DictionaryKeyConverter(JsonSerializerOptions options)
            {
                // For performance, use the existing converter if available.
                _valueConverter = (JsonConverter<TValue>)options.GetConverter(typeof(TValue));
                _keyConverter = (JsonConverter<TKey>)options.GetConverter(typeof(TKey));

                // KeyLookupCache the key and value types.
                _keyType = typeof(TKey);
                _valueType = typeof(TValue);
            }

            public override Dictionary<TKey, TValue> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.StartObject)
                {
                    throw new JsonException();
                }

                var dictionary = new Dictionary<TKey, TValue>();

                while (reader.Read())
                {
                    if (reader.TokenType == JsonTokenType.EndObject)
                    {
                        return dictionary;
                    }

                    // Get the key.
                    if (reader.TokenType != JsonTokenType.PropertyName)
                    {
                        throw new JsonException();
                    }
                    var key = _keyConverter.Read(ref reader, _keyType, options);

                    // Get the value.
                    reader.Read();
                    var value = _valueConverter.Read(ref reader, _valueType, options);

                    dictionary.Add(key, value);
                }

                throw new JsonException();
            }

            public override void Write(Utf8JsonWriter writer, Dictionary<TKey, TValue> dictionary, JsonSerializerOptions options)
            {
                writer.WriteStartObject();

                foreach (var keyValue in dictionary)
                {
                    var propertyName = keyValue.Key.ToString();
                    writer.WritePropertyName(options.PropertyNamingPolicy?.ConvertName(propertyName) ?? propertyName);
                    _valueConverter.Write(writer, keyValue.Value, options);
                }

                writer.WriteEndObject();
            }
        }

        private class QueueKeyConverter<TKey> : JsonConverter<Queue<TKey>> where TKey : struct
        {
            private readonly JsonConverter<TKey> _keyConverter;
            private readonly Type _keyType;

            public QueueKeyConverter(JsonSerializerOptions options)
            {
                _keyConverter = (JsonConverter<TKey>)options.GetConverter(typeof(TKey));
                _keyType = typeof(TKey);
            }

            public override Queue<TKey> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.StartArray)
                {
                    throw new JsonException();
                }

                var queue = new Queue<TKey>();

                while (reader.Read())
                {
                    if (reader.TokenType == JsonTokenType.EndArray)
                    {
                        return queue;
                    }

                    // Get the key.
                    if (reader.TokenType != JsonTokenType.Number)
                    {
                        throw new JsonException();
                    }
                    var key = _keyConverter.Read(ref reader, _keyType, options);

                    queue.Enqueue(key);
                }

                throw new JsonException();
            }

            public override void Write(Utf8JsonWriter writer, Queue<TKey> queue, JsonSerializerOptions options)
            {
                writer.WriteStartArray();

                foreach (var keyValue in queue)
                {
                    _keyConverter.Write(writer, keyValue, options);
                }

                writer.WriteEndArray();
            }
        }
    }
}
