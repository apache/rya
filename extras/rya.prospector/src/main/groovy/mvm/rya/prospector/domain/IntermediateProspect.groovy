package mvm.rya.prospector.domain

import org.apache.hadoop.io.WritableComparable

import static mvm.rya.prospector.domain.TripleValueType.*

/**
 * Date: 12/3/12
 * Time: 11:15 AM
 */
class IntermediateProspect implements WritableComparable<IntermediateProspect> {

    def String index
    def String data
    def String dataType
    def TripleValueType tripleValueType
    def String visibility

    @Override
    int compareTo(IntermediateProspect t) {
        if(!index.equals(t.index))
            return index.compareTo(t.index);
        if(!data.equals(t.data))
            return data.compareTo(t.data);
        if(!dataType.equals(t.dataType))
            return dataType.compareTo(t.dataType);
        if(!tripleValueType.equals(t.tripleValueType))
            return tripleValueType.compareTo(t.tripleValueType);
        if(!visibility.equals(t.visibility))
            return visibility.compareTo(t.visibility);
        return 0
    }

    @Override
    void write(DataOutput dataOutput) {
        dataOutput.writeUTF(index);
        dataOutput.writeUTF(data);
        dataOutput.writeUTF(dataType);
        dataOutput.writeUTF(tripleValueType.name());
        dataOutput.writeUTF(visibility);
    }

    @Override
    void readFields(DataInput dataInput) {
        index = dataInput.readUTF()
        data = dataInput.readUTF()
        dataType = dataInput.readUTF()
        tripleValueType = TripleValueType.valueOf(dataInput.readUTF())
        visibility = dataInput.readUTF()
    }
}
