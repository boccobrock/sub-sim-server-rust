use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex};

use super::{
    handle_client, parse_join, parse_shot, Board, ClientRegistry, Config, ShotCommand,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Spin up an in-process test server bound to an OS-assigned port.
/// Returns the bound address and the shared registry so tests can inspect it.
async fn start_test_server() -> (SocketAddr, ClientRegistry) {
    let (tx, _rx) = broadcast::channel::<String>(256);
    let registry: ClientRegistry = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let registry_clone = Arc::clone(&registry);

    tokio::spawn(async move {
        loop {
            let (socket, client_addr) = listener.accept().await.unwrap();
            let tx2 = tx.clone();
            let rx2 = tx.subscribe();
            let reg2 = Arc::clone(&registry_clone);
            tokio::spawn(async move {
                let _ = handle_client(socket, client_addr, tx2, rx2, reg2).await;
            });
        }
    });

    (addr, registry)
}

// ---------------------------------------------------------------------------
// Config tests
// ---------------------------------------------------------------------------

#[test]
fn config_default_values() {
    let cfg = Config::default();
    assert_eq!(cfg.host, "127.0.0.1");
    assert_eq!(cfg.port, 8080);
    assert_eq!(cfg.addr(), "127.0.0.1:8080");
}

#[test]
fn config_addr_formatting() {
    let cfg = Config {
        host: "0.0.0.0".to_string(),
        port: 9090,
    };
    assert_eq!(cfg.addr(), "0.0.0.0:9090");
}

// ---------------------------------------------------------------------------
// parse_join unit tests
// ---------------------------------------------------------------------------

#[test]
fn parse_join_valid() {
    let result = parse_join("J|Alice|BOARD_DATA_HERE");
    assert_eq!(
        result,
        Some(("Alice".to_string(), "BOARD_DATA_HERE".to_string()))
    );
}

#[test]
fn parse_join_board_contains_pipes() {
    // Everything after the second | is the board, including further pipe chars.
    let result = parse_join("J|Bob|a|b|c");
    assert_eq!(result, Some(("Bob".to_string(), "a|b|c".to_string())));
}

#[test]
fn parse_join_missing_board() {
    // Only one pipe — no board segment.
    assert_eq!(parse_join("J|NoBoard"), None);
}

#[test]
fn parse_join_empty_name() {
    assert_eq!(parse_join("J||some_board"), None);
}

#[test]
fn parse_join_wrong_prefix() {
    // Starts with 'J' but the prefix before the first | is not exactly "J".
    assert_eq!(parse_join("JOIN|name|board"), None);
}

#[test]
fn parse_join_non_join_message() {
    assert_eq!(parse_join("PING"), None);
    assert_eq!(parse_join("HELLO|world"), None);
}

#[test]
fn parse_join_empty_string() {
    assert_eq!(parse_join(""), None);
}

// ---------------------------------------------------------------------------
// Board & Shot unit tests
// ---------------------------------------------------------------------------

#[test]
fn board_parse_and_hit_detection() {
    // 100 character board (row 0 has 'S' at x=3, y=1; row 3 has 'X' at x=6, y=4 in 1-based coords)
    let mut raw = String::new();
    for row in 0..10 {
        for col in 0..10 {
            if row == 0 && col == 2 {
                raw.push('S');
            } else if row == 3 && col == 5 {
                raw.push('X');
            } else {
                raw.push('.');
            }
        }
    }
    assert_eq!(raw.len(), 100);
    let board = Board::parse(&raw).expect("valid 10x10 board");

    // Hit positions (non-dot) - 1-indexed
    assert!(board.is_hit(3, 1));
    assert!(board.is_hit(6, 4));

    // Miss positions (dot) - 1-indexed
    assert!(!board.is_hit(1, 1));
    assert!(!board.is_hit(3, 2));
    assert!(!board.is_hit(10, 10));

    // Out of bounds in 1-indexed system
    assert!(!board.is_hit(0, 0));
    assert!(!board.is_hit(0, 1));
    assert!(!board.is_hit(1, 0));
    assert!(!board.is_hit(11, 1));
    assert!(!board.is_hit(1, 11));
}

#[test]
fn board_parse_with_pipes_delimited_rows() {
    // 10 rows of 10 characters joined by '|'
    let row_dot = "..........";
    let row_sub = "..S.......";
    let raw = format!("{row_sub}|{row_dot}|{row_dot}|{row_dot}|{row_dot}|{row_dot}|{row_dot}|{row_dot}|{row_dot}|{row_dot}");
    let board = Board::parse(&raw).expect("valid pipe-delimited board");
    assert!(board.is_hit(3, 1));
    assert!(!board.is_hit(1, 1));
    assert!(!board.is_hit(3, 2));
}

#[test]
fn board_parse_invalid_length() {
    assert_eq!(Board::parse("short"), None);
    assert_eq!(Board::parse("....................................................................................................."), None); // 101 dots
}

#[test]
fn board_ascii_formatting() {
    let raw = ".".repeat(100);
    let board = Board::parse(&raw).unwrap();
    let ascii = board.to_ascii_string();
    assert!(ascii.contains("1 2 3 4 5 6 7 8 9 10"));
    assert!(ascii.contains("+--------------------+"));
    assert!(ascii.contains(" 1 |. . . . . . . . . .|"));
    assert!(ascii.contains("10 |. . . . . . . . . .|"));
    // Verify print_ascii doesn't panic
    board.print_ascii("Test Board");
}

#[test]
fn parse_shot_valid() {
    let shot = parse_shot("S|Alice|3|7");
    assert_eq!(
        shot,
        Some(ShotCommand {
            target_name: "Alice".to_string(),
            x: 3,
            y: 7,
        })
    );
}

#[test]
fn parse_shot_invalid() {
    assert_eq!(parse_shot(""), None);
    assert_eq!(parse_shot("SHOT|Alice|3|7"), None);
    assert_eq!(parse_shot("S|Alice|3"), None); // missing y
    assert_eq!(parse_shot("S||3|7"), None); // empty name
    assert_eq!(parse_shot("S|Alice|foo|7"), None); // non-numeric x
    assert_eq!(parse_shot("S|Alice|3|bar"), None); // non-numeric y
}

// ---------------------------------------------------------------------------
// Server behaviour tests
// ---------------------------------------------------------------------------

/// A new client should immediately receive "G\n".
#[tokio::test]
async fn client_receives_greeting_on_connect() {
    let (addr, _registry) = start_test_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line, "G\n");
}

/// A non-join message sent by one client should be echoed back to the same client.
#[tokio::test]
async fn message_is_broadcast_to_sender() {
    let (addr, _registry) = start_test_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);

    // Consume greeting
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line.trim(), "G");

    // Send a non-join command
    writer.write_all(b"HELLO\n").await.unwrap();

    // Should receive the same message back
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line.trim(), "HELLO");
}

/// A non-join message sent by client A should be received by client B.
#[tokio::test]
async fn message_is_broadcast_to_other_clients() {
    let (addr, _registry) = start_test_server().await;

    // Connect client A
    let stream_a = TcpStream::connect(addr).await.unwrap();
    let (reader_a, mut writer_a) = tokio::io::split(stream_a);
    let mut reader_a = BufReader::new(reader_a);

    // Connect client B
    let stream_b = TcpStream::connect(addr).await.unwrap();
    let (reader_b, _writer_b) = tokio::io::split(stream_b);
    let mut reader_b = BufReader::new(reader_b);

    // Consume greetings
    let mut line = String::new();
    reader_a.read_line(&mut line).await.unwrap();
    line.clear();
    reader_b.read_line(&mut line).await.unwrap();

    // Client A sends a non-join message
    writer_a.write_all(b"PING\n").await.unwrap();

    // Both A and B should receive "PING"
    let mut msg_a = String::new();
    let mut msg_b = String::new();
    reader_a.read_line(&mut msg_a).await.unwrap();
    reader_b.read_line(&mut msg_b).await.unwrap();

    assert_eq!(msg_a.trim(), "PING");
    assert_eq!(msg_b.trim(), "PING");
}

/// Multiple sequential non-join messages should all be delivered in order.
#[tokio::test]
async fn multiple_messages_delivered_in_order() {
    let (addr, _registry) = start_test_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);

    // Consume greeting
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();

    let messages = ["ONE", "TWO", "THREE"];
    for msg in &messages {
        writer
            .write_all(format!("{msg}\n").as_bytes())
            .await
            .unwrap();
    }

    for expected in &messages {
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line.trim(), *expected);
    }
}

// ---------------------------------------------------------------------------
// Join / registry tests
// ---------------------------------------------------------------------------

/// A client that sends a valid join message should appear in the registry.
/// The raw J|… message is NOT echoed; instead a J|<name> roster line is sent.
#[tokio::test]
async fn join_message_registers_client() {
    let (addr, registry) = start_test_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);

    // Consume greeting
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();

    // Send join message
    writer.write_all(b"J|Alice|my_board\n").await.unwrap();

    // Server broadcasts J|Alice (one entry in the roster) instead of the raw message
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line.trim(), "J|Alice");

    // The registry should now contain an entry for this client
    let reg = registry.lock().await;
    let info = reg.values().next().expect("expected one registered client");
    assert_eq!(info.name, "Alice");
    assert_eq!(info.board_str, "my_board");
}

/// The board field preserves everything after the second `|`, including pipes.
#[tokio::test]
async fn join_message_board_preserves_pipes() {
    let (addr, registry) = start_test_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);

    // Consume greeting
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();

    writer.write_all(b"J|Bob|row1|row2|row3\n").await.unwrap();

    // Consume the roster broadcast
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line.trim(), "J|Bob");

    let reg = registry.lock().await;
    let info = reg.values().next().expect("expected one registered client");
    assert_eq!(info.name, "Bob");
    assert_eq!(info.board_str, "row1|row2|row3");
}

/// A non-join message must NOT add the client to the registry.
#[tokio::test]
async fn non_join_message_does_not_register_client() {
    let (addr, registry) = start_test_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);

    // Consume greeting
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();

    writer.write_all(b"PING\n").await.unwrap();

    // Wait for echo
    line.clear();
    reader.read_line(&mut line).await.unwrap();

    let reg = registry.lock().await;
    assert!(reg.is_empty(), "registry should be empty for non-join messages");
}

/// A join message is NOT broadcast raw to other clients.
#[tokio::test]
async fn join_message_not_broadcast_raw() {
    let (addr, _registry) = start_test_server().await;

    // Client A
    let stream_a = TcpStream::connect(addr).await.unwrap();
    let (reader_a, mut writer_a) = tokio::io::split(stream_a);
    let mut reader_a = BufReader::new(reader_a);

    // Client B (observer)
    let stream_b = TcpStream::connect(addr).await.unwrap();
    let (reader_b, _writer_b) = tokio::io::split(stream_b);
    let mut reader_b = BufReader::new(reader_b);

    // Consume greetings
    let mut line = String::new();
    reader_a.read_line(&mut line).await.unwrap();
    line.clear();
    reader_b.read_line(&mut line).await.unwrap();

    // Client A joins
    writer_a.write_all(b"J|Alice|board\n").await.unwrap();

    // Both should receive the roster line J|Alice, NOT the raw J|Alice|board
    let mut msg_a = String::new();
    let mut msg_b = String::new();
    reader_a.read_line(&mut msg_a).await.unwrap();
    reader_b.read_line(&mut msg_b).await.unwrap();

    assert_eq!(msg_a.trim(), "J|Alice");
    assert_eq!(msg_b.trim(), "J|Alice");
    // Critically: neither contains the board data
    assert!(!msg_a.contains("board"));
    assert!(!msg_b.contains("board"));
}

/// When a second client joins, all clients receive J|<name> for every
/// registered client (i.e. the full current roster is re-broadcast),
/// followed by N|<first_joiner_name> to announce who goes first.
#[tokio::test]
async fn join_broadcasts_full_roster() {
    let (addr, _registry) = start_test_server().await;

    // Client A joins first
    let stream_a = TcpStream::connect(addr).await.unwrap();
    let (reader_a, mut writer_a) = tokio::io::split(stream_a);
    let mut reader_a = BufReader::new(reader_a);
    reader_a.read_line(&mut String::new()).await.unwrap(); // greeting
    writer_a.write_all(b"J|Alice|board_a\n").await.unwrap();
    let mut line = String::new();
    reader_a.read_line(&mut line).await.unwrap();
    assert_eq!(line.trim(), "J|Alice"); // only Alice in roster, no N| yet

    // Client B joins
    let stream_b = TcpStream::connect(addr).await.unwrap();
    let (reader_b, mut writer_b) = tokio::io::split(stream_b);
    let mut reader_b = BufReader::new(reader_b);
    reader_b.read_line(&mut String::new()).await.unwrap(); // greeting
    writer_b.write_all(b"J|Bob|board_b\n").await.unwrap();

    // After Bob joins both A and B receive 2 roster lines + 1 N| line (3 total).
    let mut roster_a: Vec<String> = Vec::new();
    let mut roster_b: Vec<String> = Vec::new();
    for _ in 0..3 {
        let mut l = String::new();
        reader_a.read_line(&mut l).await.unwrap();
        roster_a.push(l.trim().to_string());
        let mut l = String::new();
        reader_b.read_line(&mut l).await.unwrap();
        roster_b.push(l.trim().to_string());
    }

    // The two J| lines can arrive in any order; sort them for comparison.
    let mut j_lines_a: Vec<&str> = roster_a.iter().filter(|l| l.starts_with("J|")).map(|l| l.as_str()).collect();
    let mut j_lines_b: Vec<&str> = roster_b.iter().filter(|l| l.starts_with("J|")).map(|l| l.as_str()).collect();
    j_lines_a.sort();
    j_lines_b.sort();
    assert_eq!(j_lines_a, vec!["J|Alice", "J|Bob"]);
    assert_eq!(j_lines_b, vec!["J|Alice", "J|Bob"]);

    // The N| line tells everyone Alice (who joined first) goes first.
    let n_line_a = roster_a.iter().find(|l| l.starts_with("N|")).expect("expected N| line for A");
    let n_line_b = roster_b.iter().find(|l| l.starts_with("N|")).expect("expected N| line for B");
    assert_eq!(n_line_a, "N|Alice");
    assert_eq!(n_line_b, "N|Alice");
}

/// Multiple clients can join independently and both appear in the registry.
#[tokio::test]
async fn multiple_clients_registered_independently() {
    let (addr, registry) = start_test_server().await;

    // Client A
    let stream_a = TcpStream::connect(addr).await.unwrap();
    let (reader_a, mut writer_a) = tokio::io::split(stream_a);
    let mut reader_a = BufReader::new(reader_a);

    // Client B
    let stream_b = TcpStream::connect(addr).await.unwrap();
    let (reader_b, mut writer_b) = tokio::io::split(stream_b);
    let mut reader_b = BufReader::new(reader_b);

    // Consume greetings
    let mut line = String::new();
    reader_a.read_line(&mut line).await.unwrap();
    line.clear();
    reader_b.read_line(&mut line).await.unwrap();

    // Both clients join (sequentially to keep line counts deterministic)
    writer_a.write_all(b"J|Alice|board_a\n").await.unwrap();
    // Alice join: 1 roster line each
    reader_a.read_line(&mut String::new()).await.unwrap();
    reader_b.read_line(&mut String::new()).await.unwrap();

    writer_b.write_all(b"J|Bob|board_b\n").await.unwrap();
    // Bob join: 2 roster lines (Alice + Bob) + 1 N| line each
    for _ in 0..3 {
        reader_a.read_line(&mut String::new()).await.unwrap();
        reader_b.read_line(&mut String::new()).await.unwrap();
    }

    let reg = registry.lock().await;
    assert_eq!(reg.len(), 2, "both clients should be in the registry");

    let names: std::collections::HashSet<&str> = reg.values().map(|i| i.name.as_str()).collect();
    assert!(names.contains("Alice"));
    assert!(names.contains("Bob"));

    let info_alice = reg.values().find(|i| i.name == "Alice").unwrap();
    assert_eq!(info_alice.board_str, "board_a");
    let info_bob = reg.values().find(|i| i.name == "Bob").unwrap();
    assert_eq!(info_bob.board_str, "board_b");
}

/// A client's registry entry should be removed after disconnect.
#[tokio::test]
async fn client_removed_from_registry_on_disconnect() {
    let (addr, registry) = start_test_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);

    // Consume greeting
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();

    writer.write_all(b"J|Temp|temp_board\n").await.unwrap();

    // Consume the J|Temp roster broadcast
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line.trim(), "J|Temp");

    assert_eq!(registry.lock().await.len(), 1);

    // Drop the stream to close the connection
    drop(writer);
    drop(reader);

    // Give the server task a moment to clean up
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    assert!(
        registry.lock().await.is_empty(),
        "registry should be empty after client disconnects"
    );
}

/// A client info can be updated by sending a second join message.
/// The second join triggers a new roster broadcast with the updated name.
/// Since there is only one client, no N| message is sent.
#[tokio::test]
async fn join_message_updates_existing_entry() {
    let (addr, registry) = start_test_server().await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);

    // Consume greeting
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();

    // First join — roster broadcasts J|OldName
    writer.write_all(b"J|OldName|old_board\n").await.unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line.trim(), "J|OldName");

    // Second join — updates the entry, roster broadcasts J|NewName
    writer.write_all(b"J|NewName|new_board\n").await.unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line.trim(), "J|NewName");

    let reg = registry.lock().await;
    assert_eq!(reg.len(), 1, "should still be only one entry");
    let info = reg.values().next().unwrap();
    assert_eq!(info.name, "NewName");
    assert_eq!(info.board_str, "new_board");
}

/// N| names the client that joined first, not the second.
#[tokio::test]
async fn first_joiner_named_in_n_message() {
    let (addr, _registry) = start_test_server().await;

    // Client A joins first
    let stream_a = TcpStream::connect(addr).await.unwrap();
    let (reader_a, mut writer_a) = tokio::io::split(stream_a);
    let mut reader_a = BufReader::new(reader_a);
    reader_a.read_line(&mut String::new()).await.unwrap(); // greeting
    writer_a.write_all(b"J|First|board_a\n").await.unwrap();
    reader_a.read_line(&mut String::new()).await.unwrap(); // J|First roster

    // Client B joins second
    let stream_b = TcpStream::connect(addr).await.unwrap();
    let (reader_b, mut writer_b) = tokio::io::split(stream_b);
    let mut reader_b = BufReader::new(reader_b);
    reader_b.read_line(&mut String::new()).await.unwrap(); // greeting
    writer_b.write_all(b"J|Second|board_b\n").await.unwrap();

    // Collect 3 lines from each (2 J| + 1 N|)
    let mut lines_a: Vec<String> = Vec::new();
    let mut lines_b: Vec<String> = Vec::new();
    for _ in 0..3 {
        let mut l = String::new();
        reader_a.read_line(&mut l).await.unwrap();
        lines_a.push(l.trim().to_string());
        let mut l = String::new();
        reader_b.read_line(&mut l).await.unwrap();
        lines_b.push(l.trim().to_string());
    }

    let n_a = lines_a.iter().find(|l| l.starts_with("N|")).unwrap();
    let n_b = lines_b.iter().find(|l| l.starts_with("N|")).unwrap();
    // "First" joined before "Second", so they are announced as going first.
    assert_eq!(n_a, "N|First");
    assert_eq!(n_b, "N|First");
}

/// N| is only sent once, when the registry hits exactly 2. A 3rd join does
/// not trigger another N| message.
#[tokio::test]
async fn n_message_not_sent_on_third_join() {
    let (addr, _registry) = start_test_server().await;

    // Client A connects & joins
    let stream_a = TcpStream::connect(addr).await.unwrap();
    let (reader_a, mut writer_a) = tokio::io::split(stream_a);
    let mut reader_a = BufReader::new(reader_a);
    reader_a.read_line(&mut String::new()).await.unwrap(); // greeting
    writer_a.write_all(b"J|A|ba\n").await.unwrap();
    reader_a.read_line(&mut String::new()).await.unwrap(); // J|A roster

    // Client B connects & joins
    let stream_b = TcpStream::connect(addr).await.unwrap();
    let (reader_b, mut writer_b) = tokio::io::split(stream_b);
    let mut reader_b = BufReader::new(reader_b);
    reader_b.read_line(&mut String::new()).await.unwrap(); // greeting
    writer_b.write_all(b"J|B|bb\n").await.unwrap();

    // Drain the 3 lines triggered by B's join (2 roster lines + 1 N| line) from both A and B
    for _ in 0..3 {
        reader_a.read_line(&mut String::new()).await.unwrap();
        reader_b.read_line(&mut String::new()).await.unwrap();
    }

    // Client C connects & joins
    let stream_c = TcpStream::connect(addr).await.unwrap();
    let (reader_c, mut writer_c) = tokio::io::split(stream_c);
    let mut reader_c = BufReader::new(reader_c);
    reader_c.read_line(&mut String::new()).await.unwrap(); // greeting
    writer_c.write_all(b"J|C|bc\n").await.unwrap();

    // Now C's join should only broadcast 3 roster lines (J|A, J|B, J|C) to all clients and NO N| message.
    let mut lines_a: Vec<String> = Vec::new();
    let mut lines_b: Vec<String> = Vec::new();
    let mut lines_c: Vec<String> = Vec::new();
    for _ in 0..3 {
        let mut l = String::new();
        reader_a.read_line(&mut l).await.unwrap();
        lines_a.push(l.trim().to_string());
        let mut l = String::new();
        reader_b.read_line(&mut l).await.unwrap();
        lines_b.push(l.trim().to_string());
        let mut l = String::new();
        reader_c.read_line(&mut l).await.unwrap();
        lines_c.push(l.trim().to_string());
    }

    // None of the 3 lines from C's join should be an N| message.
    assert!(
        !lines_a.iter().any(|l| l.starts_with("N|")),
        "A should not receive another N| on 3rd join: {lines_a:?}"
    );
    assert!(
        !lines_b.iter().any(|l| l.starts_with("N|")),
        "B should not receive another N| on 3rd join: {lines_b:?}"
    );
    assert!(
        !lines_c.iter().any(|l| l.starts_with("N|")),
        "C should not receive N| on their own join: {lines_c:?}"
    );
}

/// A shot command is broadcast to all clients and the target's board is checked.
#[tokio::test]
async fn shot_command_broadcast_and_board_checked() {
    let (addr, registry) = start_test_server().await;

    // Connect client Alice with a 100-char board having 'S' at (3, 2)
    let stream_a = TcpStream::connect(addr).await.unwrap();
    let (reader_a, mut writer_a) = tokio::io::split(stream_a);
    let mut reader_a = BufReader::new(reader_a);
    reader_a.read_line(&mut String::new()).await.unwrap(); // greeting

    let mut alice_board = String::new();
    for y in 0..10 {
        for x in 0..10 {
            if x == 3 && y == 2 {
                alice_board.push('S');
            } else if x == 4 && y == 4 {
                alice_board.push('D');
            } else {
                alice_board.push('.');
            }
        }
    }
    writer_a
        .write_all(format!("J|Alice|{alice_board}\n").as_bytes())
        .await
        .unwrap();
    reader_a.read_line(&mut String::new()).await.unwrap(); // J|Alice

    // Connect client Bob
    let stream_b = TcpStream::connect(addr).await.unwrap();
    let (reader_b, mut writer_b) = tokio::io::split(stream_b);
    let mut reader_b = BufReader::new(reader_b);
    reader_b.read_line(&mut String::new()).await.unwrap(); // greeting

    let bob_board = ".".repeat(100);
    writer_b
        .write_all(format!("J|Bob|{bob_board}\n").as_bytes())
        .await
        .unwrap();

    // Drain the 3 lines from Bob's join (2 roster lines + 1 N| line) from both clients
    for _ in 0..3 {
        reader_a.read_line(&mut String::new()).await.unwrap();
        reader_b.read_line(&mut String::new()).await.unwrap();
    }

    // Verify registry has Alice's parsed board and it reports hit at (4, 3) (1-indexed for row 2, col 3)
    {
        let reg = registry.lock().await;
        let alice_info = reg.values().find(|c| c.name == "Alice").unwrap();
        let board = alice_info.board.as_ref().expect("Alice should have parsed board");
        assert!(board.is_hit(4, 3));
        assert!(!board.is_hit(1, 1));
    }

    // Bob sends a shot command targeting Alice at (4, 3) (HIT in 1-based indexing)
    writer_b.write_all(b"S|Alice|4|3\n").await.unwrap();

    // Both Alice and Bob should receive: S|Alice|4|3 followed by N|Alice (and no M| message)
    let mut msg_a = String::new();
    let mut msg_b = String::new();
    let mut next_a = String::new();
    let mut next_b = String::new();

    reader_a.read_line(&mut msg_a).await.unwrap();
    reader_a.read_line(&mut next_a).await.unwrap();
    reader_b.read_line(&mut msg_b).await.unwrap();
    reader_b.read_line(&mut next_b).await.unwrap();

    assert_eq!(msg_a.trim(), "S|Alice|4|3");
    assert_eq!(msg_b.trim(), "S|Alice|4|3");
    assert_eq!(next_a.trim(), "N|Alice");
    assert_eq!(next_b.trim(), "N|Alice");

    // Check that Alice's board has been updated with 'x' at (4, 3)
    {
        let reg = registry.lock().await;
        let alice_info = reg.values().find(|c| c.name == "Alice").unwrap();
        let board = alice_info.board.as_ref().unwrap();
        assert_eq!(board.0[2][3], 'x');
        // 'x' is still a non-dot character
        assert!(board.is_hit(4, 3));
    }

    // Bob sends a shot command targeting Alice at (1, 1) (MISS in 1-based indexing)
    writer_b.write_all(b"S|Alice|1|1\n").await.unwrap();

    // Both Alice and Bob should receive: S|Alice|1|1 followed by M|Alice and then N|Alice
    let mut shot_msg_a = String::new();
    let mut shot_msg_b = String::new();
    let mut miss_msg_a = String::new();
    let mut miss_msg_b = String::new();
    let mut next_msg_a = String::new();
    let mut next_msg_b = String::new();

    reader_a.read_line(&mut shot_msg_a).await.unwrap();
    reader_a.read_line(&mut miss_msg_a).await.unwrap();
    reader_a.read_line(&mut next_msg_a).await.unwrap();

    reader_b.read_line(&mut shot_msg_b).await.unwrap();
    reader_b.read_line(&mut miss_msg_b).await.unwrap();
    reader_b.read_line(&mut next_msg_b).await.unwrap();

    assert_eq!(shot_msg_a.trim(), "S|Alice|1|1");
    assert_eq!(shot_msg_b.trim(), "S|Alice|1|1");
    assert_eq!(miss_msg_a.trim(), "M|Alice");
    assert_eq!(miss_msg_b.trim(), "M|Alice");
    assert_eq!(next_msg_a.trim(), "N|Alice");
    assert_eq!(next_msg_b.trim(), "N|Alice");

    // Verify Alice's board at (1, 1) was updated with a dash '-' on miss
    {
        let reg = registry.lock().await;
        let alice_info = reg.values().find(|c| c.name == "Alice").unwrap();
        let board = alice_info.board.as_ref().unwrap();
        assert_eq!(board.0[0][0], '-');
    }
}

#[test]
fn board_set_hit_and_set_miss() {
    let raw = ".".repeat(100);
    let mut board = Board::parse(&raw).unwrap();
    assert!(!board.is_hit(5, 6));
    board.set_hit(5, 6);
    assert_eq!(board.0[5][4], 'x');
    assert!(board.is_hit(5, 6));

    board.set_miss(1, 2);
    assert_eq!(board.0[1][0], '-');
}

#[test]
fn board_has_remaining_letters() {
    let raw = ".".repeat(100);
    let mut board = Board::parse(&raw).unwrap();
    assert!(!board.has_remaining_letters());

    // Single uppercase letter
    board.0[0][0] = 'S';
    assert!(board.has_remaining_letters());

    // Hit with 'x' at (1, 1)
    board.set_hit(1, 1);
    assert!(!board.has_remaining_letters());

    // Lowercase letter at row 1, col 1 -> (2, 2)
    board.0[1][1] = 'd';
    assert!(board.has_remaining_letters());
    board.set_hit(2, 2);
    assert!(!board.has_remaining_letters());
}

/// A hit that clears all remaining letters sends N|<shooter>|GameOver.
#[tokio::test]
async fn game_over_when_all_letters_hit() {
    let (addr, registry) = start_test_server().await;

    // Connect client Alice with a board having exactly 1 ship letter 'S' at (2, 2) (index 11 in 10x10)
    let stream_a = TcpStream::connect(addr).await.unwrap();
    let (reader_a, mut writer_a) = tokio::io::split(stream_a);
    let mut reader_a = BufReader::new(reader_a);
    reader_a.read_line(&mut String::new()).await.unwrap(); // greeting

    let mut alice_board = ".".repeat(100);
    alice_board.replace_range(11..12, "S"); // (2, 2) in 1-based coordinates is index 11
    writer_a
        .write_all(format!("J|Alice|{alice_board}\n").as_bytes())
        .await
        .unwrap();
    reader_a.read_line(&mut String::new()).await.unwrap(); // J|Alice

    // Connect client Bob
    let stream_b = TcpStream::connect(addr).await.unwrap();
    let (reader_b, mut writer_b) = tokio::io::split(stream_b);
    let mut reader_b = BufReader::new(reader_b);
    reader_b.read_line(&mut String::new()).await.unwrap(); // greeting

    let bob_board = ".".repeat(100);
    writer_b
        .write_all(format!("J|Bob|{bob_board}\n").as_bytes())
        .await
        .unwrap();

    // Drain the 3 lines from Bob's join (2 roster lines + 1 N| line)
    for _ in 0..3 {
        reader_a.read_line(&mut String::new()).await.unwrap();
        reader_b.read_line(&mut String::new()).await.unwrap();
    }

    // Bob shoots Alice at (2, 2) - this destroys Alice's last ship letter
    writer_b.write_all(b"S|Alice|2|2\n").await.unwrap();

    let mut shot_msg_a = String::new();
    let mut shot_msg_b = String::new();
    let mut game_over_msg_a = String::new();
    let mut game_over_msg_b = String::new();

    reader_a.read_line(&mut shot_msg_a).await.unwrap();
    reader_a.read_line(&mut game_over_msg_a).await.unwrap();
    reader_b.read_line(&mut shot_msg_b).await.unwrap();
    reader_b.read_line(&mut game_over_msg_b).await.unwrap();

    assert_eq!(shot_msg_a.trim(), "S|Alice|2|2");
    assert_eq!(shot_msg_b.trim(), "S|Alice|2|2");
    assert_eq!(game_over_msg_a.trim(), "N|Bob|GameOver");
    assert_eq!(game_over_msg_b.trim(), "N|Bob|GameOver");

    // Board has no remaining letters
    let reg = registry.lock().await;
    let alice = reg.values().find(|c| c.name == "Alice").unwrap();
    assert!(!alice.board.as_ref().unwrap().has_remaining_letters());
}

