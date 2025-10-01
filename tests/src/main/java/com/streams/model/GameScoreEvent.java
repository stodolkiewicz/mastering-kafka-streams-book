package com.streams.model;

import java.util.Objects;

public class GameScoreEvent {
    private String gameName;
    private int score;

    public GameScoreEvent(String gameName, int score) {
        this.gameName = gameName;
        this.score = score;
    }

    public GameScoreEvent() {
    }

    public String getGameName() {
        return gameName;
    }

    public void setGameName(String gameName) {
        this.gameName = gameName;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "GameScore{" +
                "game='" + gameName + '\'' +
                ", score=" + score +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        GameScoreEvent that = (GameScoreEvent) o;
        return score == that.score && Objects.equals(gameName, that.gameName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gameName, score);
    }
}
