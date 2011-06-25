require 'rubygems'
require 'albacore'

PROJECT_DIR = Dir.pwd
OUTPUT_DIR = File.join(PROJECT_DIR, 'Output')
NUGET_DIR = File.join(PROJECT_DIR, 'Package')
SOLUTION_FILE = "NHibernate.Caches/NHibernate.Caches.Everything.sln"
#NUSPEC_FILE = File.join(OUTPUT_DIR, "NHibernate.Caches.Redis.nuspec")
NUSPEC_FILE = "Output/NHibernate.Caches.Redis.nuspec"

MSBUILD = "C:/Windows/Microsoft.NET/Framework/v4.0.30319/MSBuild.exe"
NUNIT = "#{PROJECT_DIR}/NHibernate.Caches/packages/NUnit.2.5.10.11092/tools/nunit-console.exe"
NUGET = "NHibernate.Caches/packages/NuGet.CommandLine.1.4.20615.182/tools/NuGet.exe"

raise "NUNIT exe not found at #{NUNIT}" unless File.exists? NUNIT
raise "MSBUILD exe not found at #{MSBUILD}" unless File.exists? MSBUILD
raise "NUGET exe not found at #{NUGET}" unless File.exists? NUGET

@config = ENV["CONFIG"] || 'Debug'
@build_number = ENV["build_number"] || '0.9.0'

desc "Generate assembly info"
assemblyinfo :assembly_info do |asm|
	puts "Generating assembly info with #{@build_number} version number"
	
	asm.version = @build_number
	asm.file_version = @build_number
	asm.company_name = "NHibernate.Caches.Redis"
	asm.product_name = "NHibernate.Caches.Redis"
	asm.description = "NHibernate 2nd Level Cache With Redis"
	asm.copyright = "FOSS 2011"
	asm.output_file = "NHibernate.Caches/Redis/Shared/SharedAssemblyInfo.cs"
end

desc "Compile the solution"
msbuild :compile => :assembly_info do |msb|
	puts "Compiling with #{@config} configuration, using #{MSBUILD}"

	msb.command = MSBUILD
	msb.properties  = { "Configuration" => "#{@config}", "OutputPath" => "#{OUTPUT_DIR}" }
	msb.targets :Build
	msb.verbosity = "normal"
	msb.log_level = :verbose
	msb.solution = SOLUTION_FILE
end

desc "Run the tests"	
nunit :test => :compile do |nunit|
  test_assembly = File.join(OUTPUT_DIR, "NHibernate.Caches.Redis.Tests.dll")
  puts "Running tests with #{test_assembly}"

  nunit.command = NUNIT
  nunit.assemblies test_assembly
  nunit.options "/xml=#{OUTPUT_DIR}/NHibernate.Caches.Redis.Tests.Report.xml", "/framework=4.0.30319"
end

desc "create the nuget package"
nuspec :create_spec do |nuspec|
   puts "Creating nuget spec for version #{@build_number}"
   
   nuspec.id="NHibernate.Caches.Redis"
   nuspec.version = "#{@build_number}"
   
   nuspec.authors = "Aaron Boxer, Mick Delaney"
   nuspec.description = "Redis cache provider for NHibernate. 
   https://github.com/mickdelaney/NHRedis forked from 
   https://github.com/boxerab/NHRedis. 
   "
   nuspec.title = "NHibernate.Caches.Redis"
   nuspec.language = "en-US"
   nuspec.licenseUrl = "https://github.com/boxerab/NHRedis"
   nuspec.projectUrl = "https://github.com/boxerab/NHRedis"
   
   nuspec.dependency "ServiceStack.Redis", "2.20"
   nuspec.dependency "Iesi.Collections", "3.2.0.2002"
   nuspec.dependency "NHibernate", "3.1.0.4000"
   nuspec.dependency "log4net", "1.2.10"

   nuspec.working_directory = OUTPUT_DIR
   nuspec.output_file = NUSPEC_FILE
end

desc "create the nuget package"
nugetpack :create_nuget => :create_spec do |nuget|
   puts "Creating nuget package for version #{@build_number}"

   nuget.command     = NUGET
   nuget.nuspec      = NUSPEC_FILE
   nuget.base_folder = OUTPUT_DIR
   #nuget.output      = NUGET_DIR
end












