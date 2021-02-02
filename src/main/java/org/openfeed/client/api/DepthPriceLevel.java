package org.openfeed.client.api;

import org.agrona.collections.Int2ObjectHashMap;
import org.openfeed.AddPriceLevel;
import org.openfeed.BookSide;
import org.openfeed.DeletePriceLevel;
import org.openfeed.ModifyPriceLevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class DepthPriceLevel {
    private final List<Level> bids;
    private final List<Level> asks;
    private final int bookDepth;

    public DepthPriceLevel(int bookDepth) {
        this.bookDepth = bookDepth;
        bids = new LinkedList<>();
        asks = new LinkedList<>();
        for(int i =0 ; i < bookDepth; i++) {
            bids.add(null);
            asks.add(null);
        }
    }

    public void clear() {
        for(int i =0 ; i < bookDepth; i++) {
            bids.set(i,null);
            asks.set(i, null);
        }
    }

    public void add(AddPriceLevel add) {
        List<Level> levels = get(add.getSide());
        int index = add.getLevel() - 1;
        Level l = new Level();
        l.level = add.getLevel();
        l.price = add.getPrice();
        l.qty = add.getQuantity();
        l.orderCount = add.getOrderCount();
        l.impliedQuantity = add.getImpliedQuantity();

        levels.add(index,l);
        levels.remove(bookDepth);
    }


    public void modify(ModifyPriceLevel modify) {
        List<Level> levels = get(modify.getSide());
        int index = modify.getLevel() - 1;
        Level l = levels.get(index);
        if(l != null) {
            l.qty = modify.getQuantity();
            l.orderCount = modify.getOrderCount();
            l.impliedQuantity = modify.getImpliedQuantity();
        }
    }

    public void delete(DeletePriceLevel delete) {
        List<Level> levels = get(delete.getSide());
        int index = delete.getLevel() - 1;
        levels.remove(index);
        levels.add(null);
    }

    private List<Level> get(BookSide side) {
        switch (side) {
            case UNKNOWN_BOOK_SIDE:
            case UNRECOGNIZED:
                break;
            case BID:
                return bids;
            case OFFER:
                return asks;
        }
        return null;
    }

    class Level{
        long transactionTime;
        int level;
        long price;
        long qty;
        int orderCount;
        long impliedQuantity;
    }

    public String toString() {
        final String FORMAT_STRING = "%14s %5d";

        StringBuilder builder = new StringBuilder("\n");
        for (int i = 0; i < this.bookDepth; i++) {

            Level bid = bids.get(i);
            Level ask = asks.get(i);
            if (bid == null) {
                builder.append(String.format("%4d: %5d %14s",i+1, 0, "---"));
            } else {
                builder.append(String.format("%4d: %5d %14s",i+1,bid.qty, bid.price));
            }
            builder.append(" || ");
            if (ask == null) {
                builder.append(String.format("%14s %5d", "---",0));
            } else {
                builder.append(String.format("%14s %5d", ask.price,ask.qty));
            }

            builder.append("\n");
        }

        return builder.toString();

    }



}

