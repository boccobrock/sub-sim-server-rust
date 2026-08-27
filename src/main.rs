use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Server configuration. Host and port can be overridden via environment
/// variables `SERVER_HOST` and `SERVER_PORT`.
#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
}

impl Config {
    /// Build a `Config` from environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        let host = std::env::var("SERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("SERVER_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(8080);
        Config { host, port }
    }

    /// Convenience: resolve the bind address string.
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "127.0.0.1".to_string(),
            port: 8080,
        }
    }
}

// ---------------------------------------------------------------------------
// Client registry
// ---------------------------------------------------------------------------

/// 10x10 game board represented as row-major 2D array of chars.
#[derive(Debug, Clone, PartialEq)]
pub struct Board(pub [[char; 10]; 10]);

impl Board {
    /// Parse a 10x10 board string.
    ///
    /// Accepts 100 contiguous characters (or rows separated by '|' / whitespace),
    /// filtering out delimiters and filling the 10x10 grid row-by-row.
    pub fn parse(s: &str) -> Option<Self> {
        let chars: Vec<char> = s.chars().filter(|&c| c != '|' && c != '\n' && c != '\r').collect();
        if chars.len() != 100 {
            return None;
        }
        let mut grid = [['.'; 10]; 10];
        for (i, &ch) in chars.iter().enumerate() {
            grid[i / 10][i % 10] = ch;
        }
        Some(Board(grid))
    }

    /// Check if coordinate (x, y) contains a non-dot character.
    /// x = column index (1..=10), y = row index (1..=10).
    pub fn is_hit(&self, x: usize, y: usize) -> bool {
        if (1..=10).contains(&x) && (1..=10).contains(&y) {
            self.0[y - 1][x - 1] != '.'
        } else {
            false
        }
    }

    /// Mark a hit at coordinate (x, y) with an 'x' character.
    /// x = column index (1..=10), y = row index (1..=10).
    pub fn set_hit(&mut self, x: usize, y: usize) {
        if (1..=10).contains(&x) && (1..=10).contains(&y) {
            self.0[y - 1][x - 1] = 'x';
        }
    }

    /// Mark a miss at coordinate (x, y) with a '-' character.
    /// x = column index (1..=10), y = row index (1..=10).
    pub fn set_miss(&mut self, x: usize, y: usize) {
        if (1..=10).contains(&x) && (1..=10).contains(&y) {
            self.0[y - 1][x - 1] = '-';
        }
    }

    /// Check if the board has any uppercase or lowercase ASCII letters remaining.
    /// (Hits marked as 'x' are ignored; any letter other than 'x'/'X' or non-hit letters count).
    /// More precisely, any character `c.is_ascii_alphabetic() && c != 'x' && c != 'X'` represents a remaining ship part.
    pub fn has_remaining_letters(&self) -> bool {
        self.0.iter().any(|row| {
            row.iter().any(|&c| c.is_ascii_alphabetic() && c != 'x' && c != 'X')
        })
    }

    /// Format the 10x10 board as a multi-line ASCII grid string with 1-based indexing.
    pub fn to_ascii_string(&self) -> String {
        let mut out = String::new();
        out.push_str("    1 2 3 4 5 6 7 8 9 10\n");
        out.push_str("   +--------------------+\n");
        for (y_idx, row) in self.0.iter().enumerate() {
            let row_num = y_idx + 1;
            out.push_str(&format!("{row_num:2} |"));
            for (x_idx, &ch) in row.iter().enumerate() {
                if x_idx > 0 {
                    out.push(' ');
                }
                out.push(ch);
            }
            out.push_str("|\n");
        }
        out.push_str("   +--------------------+");
        out
    }

    /// Print the 10x10 board as an ASCII grid to stdout with an optional title/label.
    pub fn print_ascii(&self, label: &str) {
        if !label.is_empty() {
            println!("--- Board: {label} ---");
        }
        println!("{}", self.to_ascii_string());
    }
}

/// Parsed shot command: `S|<target_name>|<x>|<y>`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShotCommand {
    pub target_name: String,
    pub x: usize,
    pub y: usize,
}

/// Parse a shot command string of the form `S|<target_name>|<x>|<y>`.
pub fn parse_shot(msg: &str) -> Option<ShotCommand> {
    if !msg.starts_with('S') {
        return None;
    }
    let parts: Vec<&str> = msg.split('|').collect();
    if parts.len() < 4 || parts[0] != "S" {
        return None;
    }
    let target_name = parts[1].to_string();
    if target_name.is_empty() {
        return None;
    }
    let x = parts[2].parse::<usize>().ok()?;
    let y = parts[3].parse::<usize>().ok()?;
    Some(ShotCommand { target_name, x, y })
}

/// Information stored about a connected client after they send a join message.
#[derive(Debug, Clone)]
pub struct ClientInfo {
    /// The player/client name extracted from the join message.
    pub name: String,
    /// The raw board string extracted from the join message.
    pub board_str: String,
    /// Parsed 10x10 board (if valid).
    pub board: Option<Board>,
    /// When this client sent their join message; used to determine turn order.
    pub joined_at: std::time::Instant,
}

/// Thread-safe map from socket address to client info.
/// Only populated once the client sends a valid `J|<name>|<board>` message.
pub type ClientRegistry = Arc<Mutex<std::collections::HashMap<SocketAddr, ClientInfo>>>;

/// Parse a join message of the form `J|<name>|<rest…>`.
///
/// Returns `Some((name, board))` if the message starts with `J` and contains
/// at least two `|` separators; returns `None` otherwise.
pub fn parse_join(msg: &str) -> Option<(String, String)> {
    // Must start with 'J'
    if !msg.starts_with('J') {
        return None;
    }
    let mut parts = msg.splitn(3, '|');
    let prefix = parts.next()?; // "J"
    if prefix != "J" {
        return None;
    }
    let name = parts.next()?.to_string();
    let board = parts.next()?.to_string();
    if name.is_empty() {
        return None;
    }
    Some((name, board))
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Start the TCP server and run until an error occurs.
///
/// # Arguments
/// * `config`   – bind address configuration
/// * `tx`       – broadcast sender; every message received from any client is
///                forwarded to all connected clients via this channel
/// * `registry` – shared client registry updated on join messages
pub async fn run_server(
    config: Config,
    tx: broadcast::Sender<String>,
    registry: ClientRegistry,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(config.addr()).await?;
    println!("Server listening on {}", config.addr());

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("Client connected: {addr}");

        let tx_clone = tx.clone();
        let rx = tx.subscribe();
        let registry_clone = Arc::clone(&registry);

        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, addr, tx_clone, rx, registry_clone).await {
                eprintln!("Error handling client {addr}: {e}");
            }
            println!("Client disconnected: {addr}");
        });
    }
}

/// Handle a single connected client.
///
/// 1. Immediately send the greeting `"G\n"`.
/// 2. Read newline-delimited messages from the client and broadcast each one.
///    - A message of the form `J|<name>|<board>` registers the client in the
///      shared [`ClientRegistry`] but is also broadcast like any other message.
/// 3. Forward every broadcast message (from any client) back to this client.
/// 4. Remove the client from the registry on disconnect.
pub async fn handle_client(
    mut stream: TcpStream,
    addr: SocketAddr,
    tx: broadcast::Sender<String>,
    mut rx: broadcast::Receiver<String>,
    registry: ClientRegistry,
) -> std::io::Result<()> {
    // Split so we can read and write concurrently.
    let (reader, mut writer) = stream.split();
    let mut buf_reader = BufReader::new(reader);

    // --- Greeting ---
    writer.write_all(b"G\n").await?;

    let mut line = String::new();

    loop {
        line.clear();
        tokio::select! {
            // Incoming data from this client
            result = buf_reader.read_line(&mut line) => {
                let n = result?;
                if n == 0 {
                    // EOF — client disconnected
                    break;
                }
                let msg = line.trim_end_matches('\n').trim_end_matches('\r').to_string();
                if msg.is_empty() {
                    continue;
                }
                println!("[{addr}] received: {msg:?}");

                // Check for a join message and register the client.
                if let Some((name, board_str)) = parse_join(&msg) {
                    println!("[{addr}] registered as {name:?}");
                    let board = Board::parse(&board_str);
                    if let Some(ref b) = board {
                        b.print_ascii(&format!("Client {name} ({addr}) joined"));
                    }
                    let mut reg = registry.lock().await;
                    reg.insert(
                        addr,
                        ClientInfo {
                            name,
                            board_str,
                            board,
                            joined_at: std::time::Instant::now(),
                        },
                    );
                    // Broadcast J|<name> for every currently registered client.
                    // The raw join message itself is NOT forwarded.
                    for info in reg.values() {
                        let _ = tx.send(format!("J|{}", info.name));
                    }
                    // Once exactly two clients have joined, announce who goes first.
                    if reg.len() == 2 {
                        if let Some(first) = reg.values().min_by_key(|i| i.joined_at) {
                            let _ = tx.send(format!("N|{}", first.name));
                        }
                    }
                } else if let Some(shot) = parse_shot(&msg) {
                    let mut reg = registry.lock().await;
                    let shooter_name = reg
                        .get(&addr)
                        .map(|c| c.name.clone())
                        .unwrap_or_else(|| "Unknown".to_string());
                    let target_client = reg.values_mut().find(|c| c.name == shot.target_name);
                    let mut is_hit = false;
                    let mut is_game_over = false;
                    let mut updated_board = None;
                    let mut target_name = shot.target_name.clone();

                    if let Some(client) = target_client {
                        target_name = client.name.clone();
                        if let Some(ref mut b) = client.board {
                            if b.is_hit(shot.x, shot.y) {
                                is_hit = true;
                                b.set_hit(shot.x, shot.y);
                                updated_board = Some(b.clone());
                                if !b.has_remaining_letters() {
                                    is_game_over = true;
                                }
                            } else {
                                b.set_miss(shot.x, shot.y);
                            }
                        }
                    }

                    println!(
                        "[{addr}] shot at {} ({}, {}): hit = {}",
                        shot.target_name, shot.x, shot.y, is_hit
                    );
                    if let Some(board) = updated_board {
                        board.print_ascii(&format!("HIT on {target_name} at ({}, {})", shot.x, shot.y));
                    }

                    // Forward/broadcast the shot message to all clients
                    let _ = tx.send(msg);

                    if is_game_over {
                        // After a game-ending hit, broadcast GameOver message and exit
                        let game_over_msg = format!("N|{}|GameOver", shooter_name);
                        println!("Game over! Broadcast: {}", game_over_msg);
                        let _ = tx.send(game_over_msg);
                        // Exit the application after a brief yield for clients to receive the message
                        tokio::spawn(async {
                            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                            std::process::exit(0);
                        });
                    } else {
                        // After a miss, send a message M|<client_name>
                        if !is_hit {
                            let _ = tx.send(format!("M|{}", target_name));
                        }

                        // After a non-gameover hit or miss, send N|<target_name>
                        let _ = tx.send(format!("N|{}", target_name));
                    }
                } else {
                    // Broadcast to all subscribers (including this client's rx).
                    // Ignore send errors — no subscribers is fine.
                    let _ = tx.send(msg);
                }
            }

            // Broadcast message from any client
            result = rx.recv() => {
                match result {
                    Ok(msg) => {
                        writer.write_all(format!("{msg}\n").as_bytes()).await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("[{addr}] lagged, skipped {n} messages");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    // Clean up registry entry on disconnect.
    registry.lock().await.remove(&addr);

    Ok(())
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let config = Config::from_env();
    let (tx, _rx) = broadcast::channel::<String>(256);
    let registry: ClientRegistry = Arc::new(Mutex::new(std::collections::HashMap::new()));

    if let Err(e) = run_server(config, tx, registry).await {
        eprintln!("Server error: {e}");
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// Tests (see src/tests.rs)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
