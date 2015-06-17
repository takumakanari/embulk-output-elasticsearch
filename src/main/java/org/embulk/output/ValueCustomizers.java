package org.embulk.output;


import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class ValueCustomizers {

    static interface ValueCustomizer {
        void customize(String name, String v, XContentBuilder builder) throws IOException;
    }

    static Object convet(String v, String type) {
        if (type.equals("string")) {
            return v;
        } else if (type.equals("integer")) {
            return Integer.parseInt(v);
        } else if (type.equals("long")) {
            return Long.parseLong(v);
        }
        // TODO support double, float, boolean etc ...
        throw new IllegalArgumentException(String.format("Unknown value type %s", type));
    }

    static class Array implements ValueCustomizer {

        private final String separator;
        private final String valueType; // TODO use enum

        public Array(String separator, String valueType) {
            this.separator = separator;
            this.valueType = valueType;
        }

        @Override
        public void customize(String name, String v, XContentBuilder builder)
                throws IOException {
            String[] vs = v.split(separator);
            final Object[] dest = new Integer[vs.length];
            for (int i = 0; i < vs.length; i++) {
                //dest[i] = Integer.parseInt(vs[i]);
                dest[i] = convet(vs[i], valueType);
            }
            builder.array(name, dest);
        }
    }

    static class KeyValuePair implements ValueCustomizer {

        private final String keySeparator;
        private final String itemSeparator;
        private final String valueType; // TODO use enum

        public KeyValuePair(String keySeparator, String itemSeparator, String valueType) {
            this.keySeparator = keySeparator;
            this.itemSeparator = itemSeparator;
            this.valueType = valueType;
        }

        @Override
        public void customize(String name, String v, XContentBuilder builder) throws IOException {
            ImmutableMap.Builder<String, Object> dest = ImmutableMap.builder();
            for (String pair : v.split(itemSeparator)) {
                final String[] kv = pair.split(this.keySeparator);
                if (kv.length != 2) {
                    throw new IllegalArgumentException(String.format("can not separate '%s' to key/value", pair));
                }
                dest.put(kv[0], convet(kv[1], valueType));
            }
            builder.field(name, dest.build());
        }
    }

    static ValueCustomizer create(ElasticsearchOutputPlugin.ValueCustomizeTask t) {
        if (t.getType().equals("array")) {
            return new Array(t.getItemSepatator(), t.getValueType());
        } else if (t.getType().equals("map")) {
            return new KeyValuePair(t.getKeySeparator(), t.getItemSepatator(), t.getValueType());
        }
        // TODO support other types
        throw new IllegalArgumentException(String.format("Unknown value customize format '%s'",
                t.getType()));
    }

}
