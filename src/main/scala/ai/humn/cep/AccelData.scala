package ai.humn.cep

case class AccelData(timestamp: Long, x: Float, y: Float, z: Float){
  override def toString: String = s"""{"timestamp" : ${timestamp}, "x": ${x}. "y": ${y}, "z": ${z}}"""
}
