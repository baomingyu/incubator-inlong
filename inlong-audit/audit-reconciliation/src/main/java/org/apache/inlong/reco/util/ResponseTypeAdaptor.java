/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.reco.util;

import org.apache.inlong.reco.pojo.Response;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ResponseTypeAdaptor<T> extends TypeAdapter<Response<? super Object>> {

    public static final TypeAdapterFactory FACTORY = new TypeAdapterFactory() {

        @SuppressWarnings("unchecked")
        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            if (type.getRawType() == Response.class) {
                return (TypeAdapter<T>) new ResponseTypeAdaptor<T>();
            }
            return null;
        }
    };

    private final TypeAdapter<Object> delegate = new Gson().getAdapter(Object.class);

    @Override
    public void write(JsonWriter writer, Response<? super Object> value) throws IOException {
        delegate.write(writer, value);
    }

    @Override
    public Response<? super Object> read(JsonReader reader) throws IOException {
        Response<? super Object> data = new Response<>();
        Map<String, Object> dataMap = (Map<String, Object>) readInternal(reader);

        assert dataMap != null;
        data.setSuccess((Boolean) dataMap.get("success"));
        data.setMessage((String) dataMap.get("errMsg"));
        data.setData(dataMap.get("data"));

        return data;
    }

    private Object readInternal(JsonReader reader) throws IOException {
        JsonToken token = reader.peek();
        switch (token) {
            case BEGIN_ARRAY:
                List<Object> list = new ArrayList<>();
                reader.beginArray();
                while (reader.hasNext()) {
                    list.add(readInternal(reader));
                }
                reader.endArray();
                return list;
            case BEGIN_OBJECT:
                Map<String, Object> map = new LinkedTreeMap<>();
                reader.beginObject();
                while (reader.hasNext()) {
                    map.put(reader.nextName(), readInternal(reader));
                }
                reader.endObject();
                return map;
            case STRING:
                return reader.nextString();
            case NUMBER: // 增加转换 Integer 类型的能力
                String numberStr = reader.nextString();
                if (numberStr.contains(".") || numberStr.contains("e") || numberStr.contains("E")) {
                    return Double.parseDouble(numberStr);
                }
                if (Long.parseLong(numberStr) <= Integer.MAX_VALUE) {
                    return Integer.parseInt(numberStr);
                }
                return Long.parseLong(numberStr);
            case BOOLEAN:
                return reader.nextBoolean();
            case NULL:
                reader.nextNull();
                return null;

            default:
                throw new IllegalStateException();
        }
    }

}
