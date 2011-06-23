require 'rubygems'
require 'albacore'

PROJECT_DIR = Dir.pwd
OUTPUT_DIR = File.join(PROJECT_DIR, 'Output')
SOLUTION_FILE = "NHibernate.Caches/NHibernate.Caches.Everything.sln"

MSBUILD = "C:/Windows/Microsoft.NET/Framework/v4.0.30319/MSBuild.exe"
NUNIT = "#{PROJECT_DIR}/NHibernate.Caches/packages/NUnit.2.5.10.11092/tools/nunit-console.exe"

raise "NUNIT exe not found at #{NUNIT}" unless File.exists? NUNIT
raise "MSBUILD exe not found at #{MSBUILD}" unless File.exists? MSBUILD

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















