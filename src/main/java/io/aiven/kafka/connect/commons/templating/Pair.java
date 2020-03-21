package io.aiven.kafka.connect.commons.templating;

import java.util.Objects;

public final class Pair<L, R> {

    private final L left;

    private final R right;

    private Pair(final L left, final R right) {
        this.left = left;
        this.right = right;
    }

    public L left() {
        return left;
    }

    public R right() {
        return right;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(left, pair.left)
                && Objects.equals(right, pair.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

    public static <K, M> Pair<K, M> of(final K k, final M m) {
        return new Pair<>(k, m);
    }

}
