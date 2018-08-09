package sybrix.easyom
/*
 * Copyright 2009 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * General purpose properties file reader class.
 */
public class PropertiesFile extends Properties {
        private String fileName;
        private File propFile;

        /**
         * The fileName is the classpath location of the file relative to the root '/'  of the classpath.<br/>
         * The root of the classpath starts in the classes dir <br/><br/>
         * For this path:<br/>
         * /WEB-INF/classes/env.properties <br/>
         * <p/>
         * fileName = "env.properties"
         * <p/>
         * <br/><br/>
         * For this path:<br/>
         * /WEB-INF/classes/resources/env.properties <br/>
         * fileName = "resources/env.propertiesn"
         * <br/>
         * <br/>
         * <p/>
         * To reload the file after a change has been made to it called the load method.
         *
         * @param fileName
         */
        public PropertiesFile(String fileName) {
                this.fileName = fileName;
                this.propFile = new File(fileName);
                load();
        }

        public PropertiesFile(InputStream inputStream) {
                try {
                        load(inputStream);
                } catch (Exception e) {
                        throw new RuntimeException("Properties file load for inputstream failed", e);
                } finally {
                        try {
                                inputStream.close();
                        } catch (IOException e) {
                        }
                }
        }

        /**
         * Reads the property file from the file system.
         */
        public void load() {

                try {
                        if (fileName.toLowerCase().startsWith("classpath:")) {
                                URL url = getClass().getClassLoader().getResource(fileName.substring(10));
                                FileInputStream fis = new FileInputStream(url.getFile());
                                load(fis);
                                fis.close();
                        } else {

                                Reader fis = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "ISO-8859-1"));
                                load(fis);
                                fis.close();
                        }

                } catch (Throwable e) {
                        throw new RuntimeException("Properties file load failed, fileName:" + fileName, e);
                } finally {
//                        try {
//
//                        } catch (IOException e) {
//                        }
                }
        }


        /**
         * @param key -  properties file key
         * @return Returns the value mapped to this key in the properties file.
         */
        public String getString(String key) {
                try {
                        return super.getProperty(key);
                } catch (NullPointerException e) {
                        return null;
                }
        }

        public String getString(String key, String defaultValue) {
                try {
                        return super.getProperty(key, defaultValue);
                } catch (NullPointerException e) {
                        return defaultValue;
                }
        }


        public Integer getInt(String key) {
                try {
                        return Integer.parseInt(getString(key));
                } catch (NumberFormatException e) {
                        throw new PropertyFormatException("property: " + key, e);
                } catch (NullPointerException e) {
                        return null;
                } catch (Exception e) {
                        throw new PropertyFileException("property: " + key, e);
                }
        }


        public Integer getInt(String key, Integer defaultValue) {
                try {
                        Integer val = getInt(key);
                        return val == null ? defaultValue : val;
                } catch (Exception e) {
                        return defaultValue;
                }
        }

        public Long getLong(String key) {
                try {
                        return Long.parseLong(getString(key));
                } catch (NumberFormatException e) {
                        throw new PropertyFormatException("property: " + key, e);
                } catch (NullPointerException e) {
                        return null;
                } catch (Exception e) {
                        throw new PropertyFileException("property: " + key, e);
                }
        }


        public Long getLong(String key, Long defaultValue) {
                try {
                        Long val = Long.parseLong(getString(key));
                        return val == null ? defaultValue : val;
                } catch (Exception e) {
                        return defaultValue;
                }
        }


        public Double getDouble(String key) {
                try {
                        return Double.parseDouble(getString(key));
                } catch (NumberFormatException e) {
                        throw new PropertyFormatException("property: " + key, e);
                } catch (NullPointerException e) {
                        return null;
                } catch (Exception e) {
                        throw new PropertyFileException("property: " + key, e);
                }
        }

        public Double getDouble(String key, Double defaultValue) {
                try {
                        Double val = Double.parseDouble(getString(key));
                        return val == null ? defaultValue : val;
                } catch (Exception e) {
                        return defaultValue;
                }
        }


        public Boolean getBoolean(String key) {
                try {
                        return new Boolean(getString(key));
                } catch (Exception e) {
                        throw new RuntimeException(e);
                }
        }

        public Boolean getBoolean(String key, Boolean defaultValue) {
                try {
                        return getBoolean(key);
                } catch (Exception e) {
                        return defaultValue;
                }
        }

        public String getProperty(String key) {
                return super.getProperty(key);    //To change body of overridden methods use File | Settings | File Templates.
        }

        public String getProperty(String key, String defaultValue) {
                return super.getProperty(key, defaultValue);    //To change body of overridden methods use File | Settings | File Templates.
        }


}

class PropertyFormatException extends RuntimeException {
        public PropertyFormatException() {
                super();
        }

        public PropertyFormatException(String message) {
                super(message);    //To change body of overridden methods use File | Settings | File Templates.
        }

        public PropertyFormatException(String message, Throwable cause) {
                super(message, cause);    //To change body of overridden methods use File | Settings | File Templates.
        }

        public PropertyFormatException(Throwable cause) {
                super(cause);    //To change body of overridden methods use File | Settings | File Templates.
        }
}

class PropertyFileException extends RuntimeException {
        public PropertyFileException() {
                super();
        }

        public PropertyFileException(Throwable cause) {
                super(cause);
        }

        public PropertyFileException(String message) {
                super(message);
        }

        public PropertyFileException(String message, Throwable cause) {
                super(message, cause);
        }
}
