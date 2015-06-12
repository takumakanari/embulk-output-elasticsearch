package org.embulk.output;


import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class ValueCustomizers {

    static interface ValueCustomizer {

        void customize(String name, String v, XContentBuilder builder) throws IOException;

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

}
