/*
 * Copyright (c) 2019-2023 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package rqueue.spring.example;

import java.io.File;
import java.io.IOException;
import org.apache.catalina.startup.Tomcat;

public class Main {

  private static final int PORT = 8080;

  public static void main(String[] args) throws Exception {
    String appBase = ".";
    Tomcat tomcat = new Tomcat();
    tomcat.setBaseDir(createTempDir());
    tomcat.setPort(PORT);
    tomcat.getConnector();
    tomcat.getHost().setAppBase(appBase);
    tomcat.addWebapp("", appBase);
    tomcat.start();
    tomcat.getServer().await();
  }

  // based on AbstractEmbeddedServletContainerFactory
  private static String createTempDir() {
    try {
      File tempDir = File.createTempFile("tomcat.", "." + PORT);
      tempDir.delete();
      tempDir.mkdir();
      tempDir.deleteOnExit();
      return tempDir.getAbsolutePath();
    } catch (IOException ex) {
      throw new RuntimeException(
          "Unable to create tempDir. java.io.tmpdir is set to "
              + System.getProperty("java.io.tmpdir"),
          ex);
    }
  }
}
