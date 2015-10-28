package mvm.rya.prospector.utils

/**
 * Date: 12/3/12
 * Time: 12:33 PM
 */
class CustomEntry<K, V> implements Map.Entry<K, V> {

    K key;
    V value;

    CustomEntry(K key, V value) {
        this.key = key
        this.value = value
    }

    K getKey() {
        return key
    }

    void setKey(K key) {
        this.key = key
    }

    V getValue() {
        return value
    }

    V setValue(V value) {
        this.value = value
        this.value
    }
}
