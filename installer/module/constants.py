""" Constants for the package setup module. """

BASH_PROMPT_SCRIPT = """

# Change to DEEPNOTE_HOME_DIR if set, otherwise to ~/work
if [ -n "$DEEPNOTE_HOME_DIR" ] && [ -d "$DEEPNOTE_HOME_DIR" ]; then
  cd "$DEEPNOTE_HOME_DIR"
elif [ -d "$HOME/work" ]; then
  cd "$HOME/work"
else
  echo "Directory ~/work does not exist."
fi

# Bash
if [ -n "$BASH_VERSION" ]; then
   PS1="${debian_chroot:+($debian_chroot)}\\[\\033[01;32m\\]\\u@deepnote\\[\\033[00m\\]:\\[\\033[01;34m\\]\\w\\[\\033[00m\\] # "
# Other terminals
else
   PS1="root@deepnote # "
fi
"""

GIT_SSH_COMMAND = """
ssh -i /work/.deepnote/gitkey -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
"""
