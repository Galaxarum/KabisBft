package kabis.crypto;

import bftsmart.reconfiguration.util.ECDSAKeyLoader;

import java.io.File;
import java.security.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Singleton Helper class to manage the key store.
 */
public class KeyStoreHelper {
    private static final KeyStoreHelper instance = new KeyStoreHelper();
    /**
     * The key pairs of the signers.
     * The key is the id of the signer.
     * The value is the KeyPair of the signer.
     */
    private final Map<Integer, KeyPair> keyPairs = new HashMap<>();

    /**
     * Creates a new KeyStoreHelper.
     * Loads the key pairs from the config/keysECDSA folder.
     */
    private KeyStoreHelper() {
        try {
            //TODO: Add support to multiple algorithms
            String pathName = "config/keysECDSA";
            File[] files = new File(pathName).listFiles();
            if (files == null || files.length == 0)
                throw new RuntimeException("Error loading keys - Files not found in " + pathName);
            for (File file : files) {
                String idString = file.getName().replaceAll("publickey|privatekey", "");
                int id = Integer.parseInt(idString);
                if (keyPairs.containsKey(id)) continue;
                ECDSAKeyLoader keyLoader = new ECDSAKeyLoader(id, "config", true, "ECDSA");
                PrivateKey priK = keyLoader.loadPrivateKey();
                PublicKey pubK = keyLoader.loadPublicKey();
                KeyPair kp = new KeyPair(pubK, priK);
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

    /**
     * Signs a value using the private key of the signer.
     *
     * @param signerId the id of the signer
     * @param values   the value to be signed
     * @return the signature of the value
     */
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

    /**
     * Verifies the signature of a value, using the public key of the signer.
     *
     * @param signerId the id of the signer
     * @param value    the value to be verified
     * @param sign     the signature of the value
     * @return true if the signature is valid, false otherwise
     */
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

