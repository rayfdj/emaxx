#![deny(clippy::unwrap_used)]

use std::fs;
use std::io::{self, stdout};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use crossterm::terminal;

use emaxx::batch::{self, BatchRunOptions};
use emaxx::buffer::Buffer;
use emaxx::command::{self, CommandResult};
use emaxx::display::{self, Screen};
use emaxx::keymap::{self, Key};

#[derive(Debug, Parser)]
#[command(name = "emaxx", version, disable_help_subcommand = true)]
struct Cli {
    #[arg(long)]
    batch: bool,
    #[arg(long)]
    no_init_file: bool,
    #[arg(long)]
    no_site_file: bool,
    #[arg(long)]
    no_site_lisp: bool,
    #[arg(short = 'L', value_name = "DIR")]
    load_path: Vec<PathBuf>,
    #[arg(short = 'l', value_name = "FILE")]
    load: Vec<String>,
    #[arg(long = "eval", value_name = "EXPR")]
    eval: Vec<String>,
    #[arg(value_name = "FILE")]
    file: Option<PathBuf>,
}

fn main() -> ExitCode {
    match try_main() {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(2)
        }
    }
}

fn try_main() -> Result<u8, String> {
    let cli = Cli::parse();
    if cli.batch {
        let code = batch::run_batch(BatchRunOptions {
            load_path: cli.load_path,
            load: cli.load,
            eval: cli.eval,
        })?;
        return Ok(code as u8);
    }

    if cli.no_init_file
        || cli.no_site_file
        || cli.no_site_lisp
        || !cli.load_path.is_empty()
        || !cli.load.is_empty()
        || !cli.eval.is_empty()
    {
        return Err(
            "`--no-init-file`, `--no-site-file`, `--no-site-lisp`, `-L`, `-l`, and `--eval` are only supported together with `--batch`".into(),
        );
    }

    run_interactive(cli.file).map_err(|error| error.to_string())?;
    Ok(0)
}

fn run_interactive(file: Option<PathBuf>) -> io::Result<()> {
    let mut buf = match file {
        Some(path) => {
            let contents = fs::read_to_string(&path).unwrap_or_else(|_| String::new());
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("*scratch*");
            let mut buffer = Buffer::from_text(name, &contents);
            buffer.file = Some(path.display().to_string());
            buffer.set_unmodified();
            buffer
        }
        None => Buffer::from_text("*scratch*", ""),
    };

    let global_map = keymap::default_global_keymap();
    let mut screen = Screen::new()?;
    let mut stdout = stdout();
    let mut message: Option<String> = None;
    let mut key_buffer: Vec<Key> = Vec::new();

    terminal::enable_raw_mode()?;
    crossterm::execute!(
        stdout,
        terminal::EnterAlternateScreen,
        crossterm::cursor::Show
    )?;

    display::scroll_to_cursor(&buf, &mut screen);
    display::render(&mut stdout, &buf, &screen, message.as_deref())?;

    loop {
        let key = match display::read_key()? {
            Some(key) => key,
            None => {
                screen.refresh_size()?;
                display::scroll_to_cursor(&buf, &mut screen);
                display::render(&mut stdout, &buf, &screen, message.as_deref())?;
                continue;
            }
        };

        message = None;
        key_buffer.push(key.clone());

        match global_map.lookup_seq(&key_buffer) {
            Ok(Some(command_name)) => {
                let last_char = match key_buffer.last() {
                    Some(Key::Char(character)) => Some(*character),
                    _ => None,
                };
                key_buffer.clear();

                match command::execute(&mut buf, &command_name, last_char) {
                    CommandResult::Ok => {}
                    CommandResult::Quit => break,
                    CommandResult::Error(error) => message = Some(error),
                    CommandResult::NeedInput(prompt) => {
                        message = Some(format!("{prompt} [not yet implemented]"));
                    }
                }
            }
            Ok(None) => {
                if key_buffer.len() == 1 {
                    if let Key::Char(character) = &key_buffer[0] {
                        let character = *character;
                        key_buffer.clear();
                        match command::execute(&mut buf, "self-insert-command", Some(character)) {
                            CommandResult::Ok => {}
                            CommandResult::Error(error) => message = Some(error),
                            CommandResult::Quit | CommandResult::NeedInput(_) => {}
                        }
                    } else {
                        let description = format!("{:?} is undefined", &key_buffer[0]);
                        key_buffer.clear();
                        message = Some(description);
                    }
                } else {
                    let description = format!("{:?} is undefined", &key_buffer);
                    key_buffer.clear();
                    message = Some(description);
                }
            }
            Err(()) => {
                message = Some(format!(
                    "{}-",
                    key_buffer
                        .iter()
                        .map(|key| format!("{key:?}"))
                        .collect::<Vec<_>>()
                        .join(" ")
                ));
                display::render(&mut stdout, &buf, &screen, message.as_deref())?;
                continue;
            }
        }

        display::scroll_to_cursor(&buf, &mut screen);
        display::render(&mut stdout, &buf, &screen, message.as_deref())?;
    }

    terminal::disable_raw_mode()?;
    crossterm::execute!(stdout, terminal::LeaveAlternateScreen)?;
    Ok(())
}
