package com.streams.model;

public class Enriched implements Comparable<Enriched> {
    private Long playerId;
    private Long productId;
    private String playerName;
    private String gameName;
    private Double score;

    public Enriched(ScoreWithPlayer scoreWithPlayer, Product product) {
        this.playerId = scoreWithPlayer.getPlayer().getId();
        this.productId = product.getId();
        this.playerName = scoreWithPlayer.getPlayer().getName();
        this.gameName = product.getName();
        this.score = scoreWithPlayer.getScoreEvent().getScore();
    }

    public Long getPlayerId() {
        return playerId;
    }

    public void setPlayerId(Long playerId) {
        this.playerId = playerId;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public String getGameName() {
        return gameName;
    }

    public void setGameName(String gameName) {
        this.gameName = gameName;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    @Override
    public int compareTo(Enriched o) {
        return Double.compare(o.score, score);
    }

    @Override
    public String toString() {
        return "Enriched{" +
                "playerId=" + playerId +
                ", productId=" + productId +
                ", playerName='" + playerName + '\'' +
                ", gameName='" + gameName + '\'' +
                ", score=" + score +
                '}';
    }
}
