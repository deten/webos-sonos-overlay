# v0.8.0

- Fix Close Setup. The compositor volume QML patch, bind mount, and compositor
  restart previously ran only from the boot hook, so a fresh install showed no
  on-screen indicator until the TV was power cycled. Setup now applies it as the
  last step, and the button explains that the screen will go black briefly.
- The boot hook and the setup path share one definition of those steps, so they
  cannot drift apart.
- Record webOS 6.5.0 on the LG C1 as tested, reported by a tester.
