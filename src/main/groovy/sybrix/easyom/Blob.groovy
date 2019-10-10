package sybrix.easyom

import groovy.transform.CompileStatic

@CompileStatic
class Blob {
        byte[] data
        File file
        InputStream inputStream
        java.sql.Blob blob

        public Blob(byte[] data) {
                this.data = data
        }

        public Blob(InputStream inputStream) {
                this.inputStream = inputStream
        }

        public Blob(File file) {
                this.file = file
        }

        public Blob(java.sql.Blob blob) {
                this.blob = blob
                inputStream = blob.getBinaryStream()
        }

        public InputStream toInputStream() {
                if (data != null) {
                        return new ByteArrayInputStream(data)
                } else if (file != null) {
                        return new FileInputStream(file)
                } else {
                        return inputStream
                }
        }

}
