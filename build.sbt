import AssemblyKeys._

libraryDependencies += "com.codahale" %% "jerkson" % "0.4.2"

resolvers += "Coda Hale" at "http://repo.codahale.com/"

seq(assemblySettings: _*)
