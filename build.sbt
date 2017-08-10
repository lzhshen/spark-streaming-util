import sbtassembly.AssemblyPlugin.autoImport.assemblyMergeStrategy

assemblyMergeStrategy in assembly := customMergeStrategy
ideaExcludeFolders += ".idea"
ideaExcludeFolders += ".idea_modules"