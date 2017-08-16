@0xe8ed5102687cea18;

using Go = import "/go.capnp";
$Go.package("main");
$Go.import("github.com/deciphernow/gm-fabric-go/memfd");

struct Greeting {
  text @0 :Text;
}
