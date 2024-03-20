package usecase.synthetic;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import util.T5;
import util.TypeFactory1;

public class TypeFactoryInputTuple implements TypeFactory1<InputTuple> {

    @Override
    public TypeInformation<T5<InputTuple>> getT5TypeInformation() {
        return TypeInformation.of(new TypeHint<T5<InputTuple>>() {
        });
    }

    @Override
    public TypeInformation<InputTuple> getTTypeInformation() {
        return TypeInformation.of(new TypeHint<InputTuple>() {
        });
    }

}
