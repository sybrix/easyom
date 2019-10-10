package sybrix.easyom

class ServerTimestamp extends Date implements DbServerFunction {
        public String function() {
                "CURRENT_TIMESTAMP"
        }

        public static ServerTimestamp instance(){
                return new ServerTimestamp();
        }
}
