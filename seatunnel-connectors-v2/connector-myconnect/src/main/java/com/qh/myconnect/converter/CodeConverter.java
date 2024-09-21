package com.qh.myconnect.converter;

import cn.hutool.core.util.HexUtil;
import cn.hutool.crypto.SmUtil;
import cn.hutool.crypto.asymmetric.KeyType;
import cn.hutool.crypto.asymmetric.SM2;
import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.crypto.symmetric.AES;
import cn.hutool.crypto.symmetric.SM4;
import lombok.Data;
import java.util.Map;
import java.util.function.Function;

@Data
public class CodeConverter {
    private Map<String, String> dmMap;
    private AES aes;
    private SM2 sm2;
    private SM4 sm4;

    public Function<Object, Object> dmConverter(String safeCode) {
        return str -> {
            return dmMap.get(safeCode + '.' + str);
        };
    }
    public CodeConverter(){
        byte[] key = "pangu123pangu123".getBytes();
        this.aes = new AES(key);
        byte[] privateKey = HexUtil.decodeHex("308193020100301306072a8648ce3d020106082a811ccf5501822d0479307702010104203192b1f7b849bcaef11e682b09d4d719f30b5ba43f2be6f81ac289ee2e50f9b8a00a06082a811ccf5501822da144034200041122423fd69fb39e8cb09d0269cdda139513f22c080eacda9158047ac8c6f3bd1193c01fa81dd3896c01ac9a554c4d9feacb9a80677bc493363c8b9e83f42f99");
        byte[] publicKey = HexUtil.decodeHex(
                "3059301306072a8648ce3d020106082a811ccf5501822d034200041122423fd69fb39e8cb09d0269cdda139513f22c080eacda9158047ac8c6f3bd1193c01fa81dd3896c01ac9a554c4d9feacb9a80677bc493363c8b9e83f42f99");
        this.sm2 = SmUtil.sm2(privateKey, publicKey);
        this.sm4 = new SM4(key);
    }

    public Function<Object, Object> encryptConverter(String safeCode) {
        return str -> {
            if(safeCode.equalsIgnoreCase("ENCRYPT.AES")){
                byte[] encrypt = aes.encrypt(String.valueOf(str).getBytes());
                return HexUtil.encodeHexStr(encrypt);
                /**
                 * String decryptedContent = aes.decryptStr(encryptedBytes);
                 */
            }
            if(safeCode.equalsIgnoreCase("ENCRYPT.MD5")){
                return DigestUtil.md5Hex(String.valueOf(str));
            }
            if(safeCode.equalsIgnoreCase("ENCRYPT.SM2")){
                return sm2.encryptBcd(String.valueOf(str), KeyType.PublicKey);
                /* 解密方法
                 *         byte[] privateKey = HexUtil.decodeHex("308193020100301306072a8648ce3d020106082a811ccf5501822d0479307702010104203192b1f7b849bcaef11e682b09d4d719f30b5ba43f2be6f81ac289ee2e50f9b8a00a06082a811ccf5501822da144034200041122423fd69fb39e8cb09d0269cdda139513f22c080eacda9158047ac8c6f3bd1193c01fa81dd3896c01ac9a554c4d9feacb9a80677bc493363c8b9e83f42f99");
                 *         byte[] publicKey = HexUtil.decodeHex(
                 *                 "3059301306072a8648ce3d020106082a811ccf5501822d034200041122423fd69fb39e8cb09d0269cdda139513f22c080eacda9158047ac8c6f3bd1193c01fa81dd3896c01ac9a554c4d9feacb9a80677bc493363c8b9e83f42f99");
                 *         SM2 sm2 = SmUtil.sm2(privateKey, publicKey);
                 *         String decryptStr2 = StrUtil.utf8Str(sm2.decryptFromBcd("041D73D83BCABC68CEEBD95954911375EB5962DF8928ADA906F7A6CB2005AC2C0C8349CA7E37BEA0ED68B15207A63CB5595F44B82F788513BF8BBFD255EFFCEA53C1FD201F813B858186D43CAF5A6C3A7D068FDB5C6100DB9C0152B886607AB50D2BA796E9A54F8B9854", KeyType.PrivateKey));
                 *         System.out.println(decryptStr2);
                 */
            }
            if(safeCode.equalsIgnoreCase("ENCRYPT.SM3")){
                return SmUtil.sm3(String.valueOf(str));
            }
            if(safeCode.equalsIgnoreCase("ENCRYPT.SM4")){
                return sm4.encryptHex(String.valueOf(str));
            }
            return safeCode + '.' + str;
        };
    }
}
