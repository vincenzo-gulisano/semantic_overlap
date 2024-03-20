package muSPE;

import java.util.Collection;
import java.util.function.Function;

import component.ComponentType;
import component.operator.AbstractOperator;
import component.operator.router.RouterOperator;
import stream.Stream;

public class MyRouter<T> extends AbstractOperator<T, T> implements RouterOperator<T> {

    private Function<T, Long> keyBy;
    private boolean firstInvocation = true;
    Stream<T>[] outArray;

    public MyRouter(String id, Function<T, Long> keyBy) {
        super(id, ComponentType.ROUTER);
        this.keyBy = keyBy;
    }

    @Override
    protected final void process() {

        if (isFlushed()) {
            return;
        }

        if (firstInvocation) {
            firstInvocation = false;
            outArray = new Stream[getOutputs().size()];
            int index = 0;
            for (Stream<T> output : getOutputs()) {
                outArray[index] = output;
                index++;
            }
        }

        Stream<T> input = getInput();
        T inTuple = input.getNextTuple(getIndex());

        if (isStreamFinished(inTuple, input)) {
            flush();
            return;
        }

        if (inTuple != null) {
            int i = (int) (keyBy.apply(inTuple) % outArray.length);
            outArray[i].addTuple(inTuple, getIndex());
        }
    }

    @Override
    public Collection<? extends Stream<T>> chooseOutputs(T tuple) {
        assert (false);
        return null;
    }

    @Override
    public void addOutput(Stream<T> stream) {
        state.addOutput(stream);
    }

    public Stream<T> getOutput() {
        throw new UnsupportedOperationException(
                String.format("'%s': Router has multiple outputs!", state.getId()));
    }

}
