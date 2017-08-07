package io.github.shen.streaming

/**
  * Created by shen on 8/5/17.
  */

class LazerSpec extends UnitSpec {
  val path = getClass.getResource("/exampleBeam.conf").getPath
  val lazer = new Lazer(path)

}
