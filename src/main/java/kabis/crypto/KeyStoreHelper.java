package kabis.crypto;

import bftsmart.reconfiguration.util.ECDSAKeyLoader;

import java.io.File;
import java.security.*;
import java.util.HashMap;
import java.util.Map;

public class KeyStoreHelper {
    private static final KeyStoreHelper instance = new KeyStoreHelper();
    private final Map<Integer, KeyPair> keyPairs = new HashMap<>();

    private KeyStoreHelper() {
        try {
            //TODO: Improve validation and error handling, maybe we should throw an exception if the key is not found
            //TODO: Add support to multiple algorithms
            var files = new File("config/keysECDSA").listFiles();
            for (var file : files) {
                var idString = file.getName().replaceAll("publickey|privatekey", "");
                var id = Integer.parseInt(idString);
                if (keyPairs.containsKey(id)) continue;
                var keyLoader = new ECDSAKeyLoader(id, "config", true, "ECDSA");
                var priK = keyLoader.loadPrivateKey();
                var pubK = keyLoader.loadPublicKey();
                var kp = new KeyPair(pubK, priK);
                keyPairs.put(id, kp);
                System.out.println("Adding keypair for id " + id);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error loading keys", e);
        }
    }

    public static KeyStoreHelper getInstance() {
        return instance;
    }

    public byte[] signBytes(int signerId, byte[] values) {
        try {
            //TODO: Add support to multiple algorithms
            Signature sign = Signature.getInstance("SHA256withECDSA");
            sign.initSign(keyPairs.get(signerId).getPrivate());
            sign.update(values);
            return sign.sign();
        } catch (NoSuchAlgorithmException | NullPointerException e) {
            throw new CrypthographyException("Unsupported signature algorithm", e);
        } catch (InvalidKeyException e) {
            throw new CrypthographyException("Unsupported private key", e);
        } catch (SignatureException e) {
            throw new CrypthographyException("Error computing signature", e);
        }
    }

    public boolean validateSignature(int signerId, byte[] value, byte[] sign) {
        try {
            //TODO: Add support to multiple algorithms
            Signature signature = Signature.getInstance("SHA256withECDSA");
            signature.initVerify(keyPairs.get(signerId).getPublic());
            signature.update(value);
            return signature.verify(sign);
        } catch (NoSuchAlgorithmException | NullPointerException e) {
            throw new CrypthographyException("Unsupported signature algorithm", e);
        } catch (InvalidKeyException e) {
            throw new CrypthographyException("Unsupported public key", e);
        } catch (SignatureException e) {
            throw new CrypthographyException("Error validating signature", e);
        }
    }
}

