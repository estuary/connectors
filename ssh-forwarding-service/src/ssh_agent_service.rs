use super::errors::Error;
use super::util::{get_child_stderr, get_child_stdout};
use base64::decode;
use fancy_regex::Regex;
use std::io::Write;
use std::process::{Command, Stdio};

pub struct SshAgentService {
    auth_sock: String,
    pid: u32,
}

impl SshAgentService {
    pub fn create() -> Result<Self, Error> {
        let mut child = Command::new("ssh-agent")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::piped())
            .spawn()?;

        if !child.wait()?.success() {
            return Err(Error::ExecutionError {
                cmd: "ssh-agent",
                stdout: get_child_stdout(&mut child),
                stderr: get_child_stderr(&mut child),
            });
        }
        let stdout = get_child_stdout(&mut child);
        Ok(Self {
            auth_sock: Self::parse_auth_sock(&stdout)?,
            pid: Self::parse_pid(&stdout)?,
        })
    }

    pub fn auth_sock(&self) -> &str {
        &self.auth_sock
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn add_ssh_key(&self, ssh_key_base64: &str) -> Result<(), Error> {
        assert!(
            self.started(),
            "adding ssh key before ssh agent is started."
        );

        let mut child = Command::new("ssh-add")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::piped())
            .env("SSH_AUTH_SOCK", self.auth_sock())
            .env("SSH_AGENT_PID", self.pid().to_string())
            .arg("-")
            .spawn()?;

        let child_stdin = child
            .stdin
            .as_mut()
            .expect("stdin should be available for child process.");
        child_stdin.write_all(&decode(ssh_key_base64)?)?;

        if !child.wait()?.success() {
            return Err(Error::ExecutionError {
                cmd: "ssh-add",
                stdout: get_child_stdout(&mut child),
                stderr: get_child_stderr(&mut child),
            });
        }

        Ok(())
    }

    pub fn started(&self) -> bool {
        self.pid > 0
    }

    fn parse_auth_sock(raw: &str) -> Result<String, Error> {
        let re = Regex::new(r"SSH_AUTH_SOCK=([^;]+); ").expect("failed to create regex");
        Ok(re
            .captures(&raw)?
            .ok_or(Error::RegxNoParsingResults)?
            .get(1)
            .expect("no group found")
            .as_str()
            .to_string())
    }

    fn parse_pid(raw: &str) -> Result<u32, Error> {
        let re = Regex::new(r"SSH_AGENT_PID=(\d+);").expect("failed to create regex");
        Ok(re
            .captures(&raw)?
            .ok_or(Error::RegxNoParsingResults)?
            .get(1)
            .expect("no group found")
            .as_str()
            .parse::<u32>()
            .expect("failed to parse int from string"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_RAW: &str = "
        SSH_AUTH_SOCK=/tmp/ssh-X2zamZGvROoR/agent.771088; export SSH_AUTH_SOCK;
        SSH_AGENT_PID=771089; export SSH_AGENT_PID;
    ";

    #[test]
    fn test_parse_auth_sock() {
        assert_eq!(
            SshAgentService::parse_auth_sock(TEST_RAW).unwrap(),
            "/tmp/ssh-X2zamZGvROoR/agent.771088"
        );
        assert!(matches!(
            SshAgentService::parse_pid("SSH_BAD_AUTH_SOCK=abcd;").unwrap_err(),
            Error::RegxNoParsingResults
        ));
    }
    #[test]
    fn test_parse_pid() {
        assert_eq!(SshAgentService::parse_pid(TEST_RAW).unwrap(), 771089);
        assert!(matches!(
            SshAgentService::parse_pid("SSH_AGENT_BAD_ENV=1234;").unwrap_err(),
            Error::RegxNoParsingResults
        ));
    }
}
