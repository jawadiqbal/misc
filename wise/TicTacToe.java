import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Multi-player tic-tac-toe on an N x N board where K in a row wins.
 * Each move checks only the four lines through the placed cell: O(K) per move, not O(N^2).
 * Moves are synchronized so concurrent callers can't both take a cell or play out of turn.
 *
 * Run: java TicTacToe.java
 */
public class TicTacToe {

    enum Result { CONTINUE, WIN, DRAW }

    static final class InvalidMoveException extends RuntimeException {
        InvalidMoveException(String msg) { super(msg); }
    }

    static final class Game {
        private static final int[][] DIRECTIONS = {{0, 1}, {1, 0}, {1, 1}, {1, -1}};

        private final int n;
        private final int k;
        private final List<Character> players;
        private final char[][] board;
        private int turn;
        private int moves;
        private Character winner;

        Game(int n, int k, List<Character> players) {
            if (k < 1 || k > n) throw new IllegalArgumentException("need 1 <= k <= n");
            if (players.size() < 2) throw new IllegalArgumentException("need at least 2 players");
            this.n = n;
            this.k = k;
            this.players = List.copyOf(players);
            this.board = new char[n][n];
        }

        synchronized Result move(char player, int row, int col) {
            if (winner != null || moves == n * n) throw new InvalidMoveException("game is over");
            if (players.get(turn) != player) throw new InvalidMoveException("not " + player + "'s turn");
            if (row < 0 || row >= n || col < 0 || col >= n) throw new InvalidMoveException("off board");
            if (board[row][col] != 0) throw new InvalidMoveException("cell taken");

            board[row][col] = player;
            moves++;
            if (wins(player, row, col)) {
                winner = player;
                return Result.WIN;
            }
            if (moves == n * n) return Result.DRAW;
            turn = (turn + 1) % players.size();
            return Result.CONTINUE;
        }

        synchronized Character winner() { return winner; }

        synchronized char currentPlayer() { return players.get(turn); }

        private boolean wins(char p, int row, int col) {
            for (int[] d : DIRECTIONS) {
                int count = 1 + count(p, row, col, d[0], d[1]) + count(p, row, col, -d[0], -d[1]);
                if (count >= k) return true;
            }
            return false;
        }

        private int count(char p, int row, int col, int dr, int dc) {
            int c = 0;
            for (int r = row + dr, cc = col + dc;
                 c < k - 1 && r >= 0 && r < n && cc >= 0 && cc < n && board[r][cc] == p;
                 r += dr, cc += dc) {
                c++;
            }
            return c;
        }
    }

    public static void main(String[] args) throws Exception {
        Game g = new Game(3, 3, List.of('X', 'O'));
        g.move('X', 0, 0);
        g.move('O', 1, 0);
        g.move('X', 1, 1);
        g.move('O', 2, 0);
        check(g.move('X', 2, 2) == Result.WIN && g.winner() == 'X', "diagonal win");
        expectInvalid(() -> g.move('O', 0, 1));

        Game anti = new Game(5, 4, List.of('A', 'B', 'C'));
        int[][] script = {{0, 4}, {0, 0}, {4, 4}, {1, 3}, {1, 0}, {4, 3}, {2, 2}, {2, 0}, {4, 2}};
        char[] who = {'A', 'B', 'C'};
        for (int i = 0; i < script.length; i++) anti.move(who[i % 3], script[i][0], script[i][1]);
        check(anti.move('A', 3, 1) == Result.WIN, "anti-diagonal, 3 players, k < n");

        Game rules = new Game(3, 3, List.of('X', 'O'));
        rules.move('X', 1, 1);
        expectInvalid(() -> rules.move('X', 0, 0));
        expectInvalid(() -> rules.move('O', 1, 1));
        expectInvalid(() -> rules.move('O', 3, 0));

        Game draw = new Game(3, 3, List.of('X', 'O'));
        int[][] d = {{0, 0}, {0, 1}, {0, 2}, {1, 1}, {1, 0}, {1, 2}, {2, 1}, {2, 0}, {2, 2}};
        Result last = null;
        for (int i = 0; i < d.length; i++) last = draw.move(i % 2 == 0 ? 'X' : 'O', d[i][0], d[i][1]);
        check(last == Result.DRAW, "full board, no line");

        Game race = new Game(3, 3, List.of('X', 'O'));
        AtomicInteger accepted = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        Thread[] threads = new Thread[16];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    start.await();
                    race.move('X', 0, 0);
                    accepted.incrementAndGet();
                } catch (InvalidMoveException | InterruptedException ignored) {
                }
            });
            threads[i].start();
        }
        start.countDown();
        for (Thread t : threads) t.join();
        check(accepted.get() == 1 && race.currentPlayer() == 'O', "one concurrent move wins the cell");

        System.out.println("TicTacToe: all checks passed");
    }

    static void expectInvalid(Runnable r) {
        try { r.run(); } catch (InvalidMoveException e) { return; }
        throw new AssertionError("expected invalid move");
    }

    static void check(boolean ok, String msg) { if (!ok) throw new AssertionError(msg); }
}
