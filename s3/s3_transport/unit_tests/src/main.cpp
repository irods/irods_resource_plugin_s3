#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

extern std::string keyfile;
extern std::string hostname;
extern std::string bucket_name;

int main( int argc, char* argv[] )
{
  Catch::Session session; // There must be exactly one instance

  // writing to session.configData() here sets defaults
  // this is the preferred way to set them

  // Build a new parser on top of Catch's
  using namespace Catch::clara;
  auto cli
    = session.cli() // Get Catch's composite command line parser
    | Opt( hostname, "hostname" )
        ["--hostname"]
        ("the S3 host (default: s3.amazonaws.com)")
    | Opt( keyfile, "keyfile" )
        ["--keyfile"]
        ("the file holding the access key and secret access key");

  // Now pass the new composite back to Catch so it uses that
  session.cli( cli );

  int returnCode = session.applyCommandLine( argc, argv );
  if( returnCode != 0 ) // Indicates a command line error
        return returnCode;

  // writing to session.configData() or session.Config() here
  // overrides command line args
  // only do this if you know you need to

  int numFailed = session.run();

  // numFailed is clamped to 255 as some unices only use the lower 8 bits.
  // This clamping has already been applied, so just return it here
  // You can also do any post run clean-up here
  return numFailed;
}

