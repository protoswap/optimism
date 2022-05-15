import React from 'react';
import './App.css';

import { ethers } from 'ethers'
import { ChugSplashRegistryABI, ChugSplashManagerABI } from '@eth-optimism/chugsplash/dist/autogen/ifaces'

interface TState {
  registry: ethers.Contract
  provider: ethers.providers.JsonRpcProvider
  managers: ethers.Contract[]
}

interface TProps {

}

class App extends React.Component<TProps, TState> {
  constructor(props: TProps) {
    super(props);

    const provider = new ethers.providers.JsonRpcProvider('')
    const registry = new ethers.Contract(
      '',
      ChugSplashRegistryABI,
      provider
    )

    this.state = {
      provider: provider,
      registry: registry,
      managers: [],
    }
  }

  async componentDidMount() {
    const events = await this.state.registry.queryFilter(
      this.state.registry.filters.ChugSplashProjectRegistered()
    )

    const managers = []
    for (const event of events) {
      managers.push(
        new ethers.Contract(
          event.args.
      )
    }
  }

  render() {
    return (
      <div className="App">

      </div>
    )
  }
}

export default App;
