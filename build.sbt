import sbtassembly.AssemblyPlugin.autoImport.assemblyMergeStrategy

assemblyMergeStrategy in assembly := customMergeStrategy
ideaExcludeFolders += ".idea"
ideaExcludeFolders += ".idea_modules"

(dependencyClasspath in Test) := (dependencyClasspath in Test).map(
  _.filterNot(_.data.name.contains("slf4j-log4j12"))
).value

