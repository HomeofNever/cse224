{ sources ? import ./nix/sources.nix }: # import the sources
with
{
  overlay = _: pkgs:
    {
      niv = (import sources.niv { }).niv;
    };
};
let
  pkgs = (import sources.nixpkgs) {
    overlays = [ overlay ];
    config.allowUnfree = true;
  };

in
pkgs.mkShell {
  buildInputs = with pkgs; [
    bash
    go
    
    nixpkgs-fmt

    niv
  ];

  shellHook = ''
    # Before run command
    echo 'Run "make help" to see available targets.'
  '';
}