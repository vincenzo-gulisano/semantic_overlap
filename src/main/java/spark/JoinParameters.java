package spark;

public class JoinParameters {

    public static long getWA(ExperimentID id) {
        switch (id) {
            case LLJ:
                return 1;
            case ALJ:
                return 1;
            case HLJ:
                return 1;
            case LHJ:
                return 1;
            case AHJ:
                return 1;
            case HHJ:
                return 1;
            default:
                throw new RuntimeException("Unrecognized experiment id");
        }
    }

    public static long getWS(ExperimentID id) {
        switch (id) {
            case LLJ:
                return 3;
            case ALJ:
                return 3;
            case HLJ:
                return 3;
            case LHJ:
                return 10;
            case AHJ:
                return 10;
            case HHJ:
                return 10;
            default:
                throw new RuntimeException("Unrecognized experiment id");
        }
    }

    public static long getMinLength(ExperimentID id) {
        switch (id) {
            case LLJ:
                return 210;
            case ALJ:
                return 150;
            case HLJ:
                return 100;
            case LHJ:
                return 210;
            case AHJ:
                return 150;
            case HHJ:
                return 100;
            default:
                throw new RuntimeException("Unrecognized experiment id");
        }
    }

}