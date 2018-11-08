package aldwxutils

object JudgeVersion {
  def version(v:Any):Boolean={
    if(v.toString>="7.0.0"){
      return true
    }else{
      return false
    }
  }
}
