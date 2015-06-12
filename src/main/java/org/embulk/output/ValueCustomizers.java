package org.embulk.output;


import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class ValueCustomizers {

    static interface ValueCustomizer {

        void customize(String name, String v, XContentBuilder builder) throws IOException;

    }

    static class StringArray implements ValueCustomizer {

        private final String separator;

        public StringArray(String separator) {
            this.separator = separator;
        }

        @Override
        public void customize(String name, String v, XContentBuilder builder)
                throws IOException {
            builder.array(name, v.split(separator));
        }

    }

    static class IntArray implements ValueCustomizer {

        private final String separator;

        public IntArray(String separator) {
            this.separator = separator;
        }

        @Override
        public void customize(String name, String v, XContentBuilder builder)
                throws IOException {
            String[] vs = v.split(separator);
            final Integer[] dest = new Integer[vs.length];
            for (int i = 0; i < vs.length; i++) {
                dest[i] = Integer.parseInt(vs[i]);
            }
            builder.array(name, dest);
        }
    }

    static ValueCustomizer create(ElasticsearchOutputPlugin.ValueCustomizeTask t) {
        if (t.getType().equals("int_array")) {
            return new IntArray(t.getValueSepatator());
        } else if (t.getType().equals("string_array")) {
            return new StringArray(t.getValueSepatator());
        }
        // TODO support other types
        throw new IllegalArgumentException(String.format("Unknown value customize format '%s'",
                t.getType()));
    }

}
