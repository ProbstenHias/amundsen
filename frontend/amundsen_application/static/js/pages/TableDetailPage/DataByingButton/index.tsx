import * as React from 'react';
import { connect } from 'react-redux';
import { bindActionCreators } from 'redux';

import { TableMetadata } from 'interfaces';
import { GlobalState } from 'ducks/rootReducer';

// TODO: Use css-modules instead of 'import'
import './styles.scss';


export interface BuyNowButtonProps {
  tableData: TableMetadata;
}


export class BuyNowButton extends React.Component<BuyNowButtonProps> {
  handleClick = () => {
    const { database, schema, name, cluster } = this.props.tableData;
    const queryParams = new URLSearchParams({
      source_db_type: database,
      source_schema: schema,
      source_table: name,
      source_db: cluster
    });
    const url = `http://127.0.0.1:5006/?${queryParams.toString()}`;
    window.open(url, '_blank');
  };

  render() {
    return (
      <button
        id="buy-now-button"
        className="btn btn-default btn-lg"
        onClick={this.handleClick}
      >
        Buy Now
      </button>
    );
  }
}



export default BuyNowButton;