package enums;

import lombok.Getter;


public enum LockType {

    SHARE_LOCK(0, "共享锁"),
    EXCLUSIVE_LOCK(1, "排它锁");
    @Getter
    private Integer code;
    @Getter
    private String value;

    LockType(int code, String value) {
        this.code = code;
        this.value = value;
    }
}
