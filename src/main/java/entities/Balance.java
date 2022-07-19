package entities;

import lombok.Builder;

import java.util.UUID;

@Builder
public class Balance {

    private UUID accountId;

    private Integer balance;

}
