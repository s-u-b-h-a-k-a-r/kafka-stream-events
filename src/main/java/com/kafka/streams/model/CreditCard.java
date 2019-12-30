package com.kafka.streams.model;

import static javaslang.API.Case;
import static javaslang.collection.List.ofAll;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.kafka.streams.events.CardRepaid;
import com.kafka.streams.events.CardWithdrawn;
import com.kafka.streams.events.DomainEvent;
import com.kafka.streams.events.LimitAssigned;

import javaslang.API;
import javaslang.Predicates;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@ToString
@Getter
public class CreditCard {

    private UUID uuid;
    private BigDecimal limit;
    private BigDecimal used = BigDecimal.ZERO;

    public List<DomainEvent> getDirtyEvents() {
        return Collections.unmodifiableList(dirtyEvents);
    }

    private final List<DomainEvent> dirtyEvents = new ArrayList<>();

    public CreditCard(UUID uuid) {
        this.uuid = uuid;
    }

    public void assignLimit(BigDecimal amount) { // command
        if (limitAlreadyAssigned()) { // invariant
            throw new IllegalStateException(); // NACK
        }
        // ACK
        limitAssigned(new LimitAssigned(uuid, new Date(), amount));
    }

    private CreditCard limitAssigned(LimitAssigned limitAssigned) {
        this.limit = limitAssigned.getLimit();
        dirtyEvents.add(limitAssigned);
        return this;
    }

    private boolean limitAlreadyAssigned() {
        return limit != null;
    }

    public void withdraw(BigDecimal amount) {
        if (notEnoughMoneyToWithdraw(amount)) {
            throw new IllegalStateException();
        }
        cardWithdrawn(new CardWithdrawn(uuid, amount, new Date()));
    }

    private CreditCard cardWithdrawn(CardWithdrawn cardWithdrawn) {
        this.used = used.add(cardWithdrawn.getAmount());
        dirtyEvents.add(cardWithdrawn);
        return this;

    }

    private boolean notEnoughMoneyToWithdraw(BigDecimal amount) {
        return availableLimit().compareTo(amount) < 0;
    }

    public void repay(BigDecimal amount) {
        cardRepaid(new CardRepaid(uuid, amount, new Date()));
    }

    private CreditCard cardRepaid(CardRepaid cardRepaid) {
        used = used.subtract(cardRepaid.getAmount());
        dirtyEvents.add(cardRepaid);
        return this;
    }

    public BigDecimal availableLimit() {
        return limit.subtract(used);
    }

    public void eventsFlushed() {
        dirtyEvents.clear();
    }

    public static CreditCard recreate(UUID uuid, List<DomainEvent> events) {
        return ofAll(events).foldLeft(new CreditCard(uuid), CreditCard::handle);
    }

    public CreditCard handle(DomainEvent domainEvent) {
        return API.Match(domainEvent).of(Case(Predicates.instanceOf(LimitAssigned.class), this::limitAssigned),
                Case(Predicates.instanceOf(CardRepaid.class), this::cardRepaid),
                Case(Predicates.instanceOf(CardWithdrawn.class), this::cardWithdrawn)

        );
    }
}
