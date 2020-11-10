package org.hypertrace.viewcreator;

/**
 * All the logic of the view-creator is in the view-creator-framework module. The purpose of this
 * module is to take the view-creator-framework and view generator api module with the avro schemas
 * and package them into the resulting docker image. This class is here to ensure that the docker
 * image builder sets the class path correctly. TODO: Research a better way to do this. Maybe have
 * this class verify that all the classes in the configs are available at runtime.
 */
public class Dummy {

  public void doNothing() {
  }
}
