

JAVA_BIN="/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/java"
OPEN_OPTS="--add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/java.util.zip=ALL-UNNAMED"

run_in_terminal() {
	local TITLE="$1"
	local CMD="$2"
	osascript <<EOF
tell application "Terminal"
	activate
	do script "echo '$TITLE' && $CMD"
end tell
EOF
}

run_in_terminal "Starting ServerApplication..." "$JAVA_BIN $OPEN_OPTS @/var/folders/cz/tfnnlhxn7336s2k9y09d4f5c0000gn/T/cp_2d60ktrhyzmpc680dyfduhqos.argfile com.github.kkomitski.opal.server.ServerApplication"
run_in_terminal "Starting AeronMediaDriver..." "$JAVA_BIN $OPEN_OPTS @/var/folders/cz/tfnnlhxn7336s2k9y09d4f5c0000gn/T/cp_aggbc4cl7caujmnbzu4j4meqf.argfile com.github.kkomitski.opal.aeron.AeronMediaDriver"
run_in_terminal "Starting MessagingService..." "$JAVA_BIN $OPEN_OPTS @/var/folders/cz/tfnnlhxn7336s2k9y09d4f5c0000gn/T/cp_j3eekgp8okggvuo8zbo8xtef.argfile com.github.kkomitski.opal.messaging.MessagingService"
run_in_terminal "Starting MatchingEngine..." "$JAVA_BIN $OPEN_OPTS @/var/folders/cz/tfnnlhxn7336s2k9y09d4f5c0000gn/T/cp_8j16j8llbs85yk51jxcvan52c.argfile com.github.kkomitski.opal.MatchingEngine"

