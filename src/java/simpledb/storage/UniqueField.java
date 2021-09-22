package simpledb.storage;

import java.io.DataOutputStream;
import java.io.IOException;

import simpledb.common.Type;

/**
 * 只能和自己相等的Field, 用于作为Map的key使用
 * 
 * 单例
 */
public class UniqueField implements Field {
    private static final UniqueField INSTANCE = new UniqueField();

    public static Field getInstance() {
        return INSTANCE;
    }

    private UniqueField() {}

    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compare(simpledb.execution.Predicate.Op op, Field value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }
}
