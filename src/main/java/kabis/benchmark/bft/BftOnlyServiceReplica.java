package kabis.benchmark.bft;

import kabis.validation.KabisServiceReplica;
import kabis.validation.SecureIdentifier;

public class BftOnlyServiceReplica extends KabisServiceReplica {
    public BftOnlyServiceReplica(int id) {
        super(id);
    }

    public static void main(String[] args) {
        SecureIdentifier.setUseSignatures(false);
        KabisServiceReplica.main(args);
    }
}
